import os
import time
import unittest
from threading import Thread

from . import rabbitmq


def setup_rabbitmq_envs():
    os.environ["RABBITMQ_SERVER_URL"] = "localhost"


class TestRabbitMQ(unittest.TestCase):
    def setUp(self):
        setup_rabbitmq_envs()
        self.queue = "test"
        self.consumer = rabbitmq.Consumer(self.queue, self.handler)
        self.thread = Thread(target=self.consumer.start)
        self.received_msg = None

    def tearDown(self):
        self.consumer.stop()
        self.thread.join()

    def handler(self, payload):
        self.received_msg = payload.decode("utf-8")

    def wait_until_channel_opened(self):
        timeout = time.time() + 15
        while time.time() < timeout:
            if self.consumer.channel:
                break
            time.sleep(1)

    def wait_until_message_received(self):
        timeout = time.time() + 15
        while time.time() < timeout:
            if self.received_msg is not None:
                break
            time.sleep(1)

    def test_publish(self):
        # start consumer on a different thread
        print("starting consumer....")
        self.thread.start()

        print("thread has started....")
        # wait until connection has opened
        self.wait_until_channel_opened()

        print("Publishing a message.....")
        publisher = rabbitmq.Publisher(self.queue)
        msg = 'Hello world!'
        try:
            publisher.publish(msg)
        except Exception as e:
            self.fail(e)

        self.wait_until_message_received()

        self.assertEqual(msg, self.received_msg)


if __name__ == '__main__':
    unittest.main()
