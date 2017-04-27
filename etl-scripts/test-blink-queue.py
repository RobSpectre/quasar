from QuasarQueue import QuasarQueue
import logging


if __name__ == "__main__":
    log_format = "%(asctime)s - %(levelname)s: %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)

    testQuasarQueue = QuasarQueue()
    testQuasarQueue.start()
