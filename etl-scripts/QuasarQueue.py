import pika
import config
import json

import MySQLdb


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
                 queue_url=config.blink_queue_address,
                 queue_name=config.blink_queue_name,
                 mysql_host=config.MYSQL_HOST,
                 mysql_port=config.MYSQL_PORT,
                 mysql_user=config.MYSQL_USER,
                 mysql_password=config.MYSQL_PASSWORD,
                 mysql_database=config.MYSQL_DATABASE,
                 mysql_table=config.MYSQL_TABLE):
        self.params = pika.URLParameters(queue_url)
        self.params.socket_timeout = 5
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue_name, durable=True)

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

    def on_message(self, channel, method_frame, header_frame, body):
        print(method_frame.delivery_tag)
        print(body)
        print(channel.basic_ack(delivery_tag=method_frame.delivery_tag))

    def getMessages(self):
        self.channel.basic_consume(self.on_message, config.blink_queue_name)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
        self.connection.close()

    def getOneMessage(self):
        queue = config.blink_queue_name
        method_frame, header_frame, body = self.channel.basic_get(queue)
        if method_frame:
            message_response = body.decode()
            message_data = json.loads(message_response)
            return self.insert_record(message_data)
        else:
            print("No messages in queue!")

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

    def mysql_query(self, query):
        try:
            self.mysql_cursor.execute(query)
            return self.mysql_connection.commit()
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
        print("ERROR: {0}".format(message))
