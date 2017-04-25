import pika
import config

class BlinkQueue:
    """ This class handles queue tasks for Blink Quasar Customer.io Queue.

    This first class is to MVP our migration to Customer.io using queueing
    in our ETL pipeline, a first for DS.

    Basic setup for now is assuming only connecting to a single queue.
    For future version, re-factoring so this class handles all the queue
    connection niceties and can connect to any queue URI using pika is
    probably better.
    """

    def __init__(self, queue_url=config.blink_queue_address, queue_name=config.blink_queue_name):
        self.params = pika.URLParameters(queue_url)
        self.params.socket_timeout = 5
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue_name, durable=True)

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
