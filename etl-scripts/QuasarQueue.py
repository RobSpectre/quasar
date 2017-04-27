import json
import logging
import MySQLdb
from queue import Queue
import time

import pika

import config

log_format = "%(asctime)s - %(levelname)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format)


class QuasarQueue:
    """ This class handles queue tasks for Blink Quasar Customer.io Queue.

    This first class is to MVP our migration to Customer.io using queueing
    in our ETL pipeline, a first for DS.

    Basic setup for now is assuming only connecting to a single queue.
    For future version, re-factoring so this class handles all the queue
    connection niceties and can connect to any queue URI using pika is
    probably better.
    """

    def __init__(self,
                 amqp_uri=config.AMQP_URI,
                 amqp_queue=config.AMQP_QUEUE,
                 amqp_exchange=config.AMQP_EXCHANGE,
                 mysql_host=config.MYSQL_HOST,
                 mysql_port=config.MYSQL_PORT,
                 mysql_user=config.MYSQL_USER,
                 mysql_password=config.MYSQL_PASSWORD,
                 mysql_database=config.MYSQL_DATABASE,
                 mysql_table=config.MYSQL_TABLE):
        self.params = pika.URLParameters(amqp_uri)
        self.params.socket_timeout = 5
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=100)
        self.channel.queue_declare(amqp_queue, durable=True)
        self.retry_queue = Queue()

        self.amqp_uri = amqp_uri
        self.amqp_exchange = amqp_exchange
        self.amqp_queue = amqp_queue
        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_database = mysql_database
        self.mysql_table = mysql_table

        self.mysql_connection = self._create_connection(mysql_host,
                                                        mysql_port,
                                                        mysql_user,
                                                        mysql_password,
                                                        mysql_database)
        self.mysql_cursor = self.mysql_connection.cursor()

    def start(self):
        logging.info("Starting Blink consumer...")
        self.channel.basic_consume(self.on_message, self.amqp_queue)
        try:
            self.channel.start_consuming()
            logging.info("Blink consumer started.")
        except KeyboardInterrupt:
            self.channel.stop_consuming()
        self.connection.close()

    def on_message(self, channel, method_frame, header_frame, body):
        message_data = self._body_decode(body)
        logging.info("[Message {0}]: Received."
                     "".format(message_data['meta']['request_id']))

        if not self.retry_queue.empty():
            message = self.retry_queue.get()
            if message['message_data']['meta']['retry_after'] < time.time():
                self._process_message(message['method_frame'],
                                      message['message_data'])
            else:
                self.retry_queue.put(message)

        return self._process_message(method_frame, message_data)

    def _process_message(self, method_frame, message_data):
        logging.info("[Message {0}] Processing message..."
                     "".format(message_data['meta']['request_id']))
        query_results = self.insert_record(message_data)

        if query_results:
            self.channel.basic_ack(method_frame.delivery_tag)
            logging.info("[Message {0}] Message processed."
                         "".format(message_data['meta']['request_id']))
            return True
        else:
            logging.info("[Message {0}] Message failed, retrying..."
                         "".format(message_data['meta']['request_id']))
            return self._retry_message(method_frame, message_data)

    def _retry_message(self, method_frame, message_data):
        if message_data['meta'].get('retry', None):
            compute_retry = (message_data['meta']['retry'] ^ 2) * 30
            message_data['meta']['retry'] = compute_retry
        else:
            message_data['meta']['retry'] = 1

        message_data['meta']['retry_after'] = time.time() + \
            message_data['meta']['retry']

        self.channel.basic_ack(method_frame.delivery_tag)

        return self.retry_queue.put({'method_frame': method_frame,
                                     'message_data': message_data})

    def _create_connection(self, mysql_host, mysql_port, mysql_user,
                           mysql_password, mysql_database):
        try:
            conn = MySQLdb.connect(host=mysql_host,
                                   port=mysql_port,
                                   user=mysql_user,
                                   passwd=mysql_password,
                                   db=mysql_database,
                                   use_unicode=True,
                                   charset='utf8')
        except MySQLdb.InterfaceError as e:
            conn = False
            raise QuasarQueueException(e)
        finally:
            return conn

    def _create_disconnect(self):
        self.mysql_cursor.close()
        return self.mysql_connection()

    def _body_decode(self, body):
        message_response = body.decode()

        try:
            return json.loads(message_response)
        except Exception as e:
            raise QuasarQueueException(e)

    def _body_encode(self, message_data):
        return json.dumps(message_data)

    def mysql_query(self, query):
        try:
            self.mysql_cursor.execute(query)
            self.mysql_connection.commit()
            return True
        except MySQLdb.DatabaseError as e:
            raise QuasarQueueException(e)

    def insert_record(self, message_object):
        email_address = message_object['data']['data']['email_address']
        customer_id = message_object['data']['data']['customer_id']

        return self.mysql_query("INSERT INTO {2} "
                                "VALUES(\"{0}\", \"{1}\");"
                                "".format(email_address,
                                          customer_id,
                                          self.mysql_table))


class QuasarQueueException(Exception):
    def __init__(self, message):
        logging.error("ERROR: {0}".format(message))
        pass
