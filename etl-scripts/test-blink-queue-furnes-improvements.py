from QuasarQueue import BlinkQueue
import config

# Handle a message from the queue.
def on_message(self, channel, method_frame, header_frame, body):
    # TODO: Transform the data to the way want it.
    print(body)
    # ...

    # TODO: Send these values to MySQL
    print(method_frame.delivery_tag)
    # ...


# Do the things!
queue = BlinkQueue(config.blink_queue_address, config.blink_queue_name)
queue.getMessages(on_message)









# Option 2: run a scheduled job to empty the queue
# while (testBlink.hasMessages()) {
#     message = testBlink.getOneMessage();
#
#     bladeDatabase.insert(message);
#     testBlink.remove(message.id);
# }
