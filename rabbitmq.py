import os
import json
import time

import pika
import logging
import functools

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(level=LOG_LEVEL)


class Consumer:
    def __init__(self, handler):
        self.server_url = os.getenv("RABBITMQ_SERVER_URL")
        self.queue = os.getenv("INPUT_QUEUE")

        # Connect to RabbitMQ using the default parameters
        self.connection = None
        self.channel = None
        self.handler = handler
        self.consuming = False
        self.closing = False
        self.consumer_tag = None
        self.should_reconnect = False

    def start(self):
        self.connection = pika.SelectConnection(
            pika.ConnectionParameters(host=self.server_url),
            on_open_callback=self.on_connected,
            on_close_callback=self.on_connection_closed,
        )
        # Loop so we can communicate with RabbitMQ
        self.connection.ioloop.start()

    def on_connected(self, unused_connection):
        """Called when we are fully connected to RabbitMQ"""
        logging.info("Connected to RabbitMQ.........")
        # Open a channel
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """Called when our channel has opened"""
        logging.info("Channel opened....")
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.queue_declare(
            queue=self.queue,
            durable=True,
            exclusive=False,
            auto_delete=False,
            callback=self.on_queue_declared,
        )

    def on_queue_declared(self, frame):
        """Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ"""
        logging.info("Consumer is ready to consume.....")
        self.consuming = True
        self.consumer_tag = self.channel.basic_consume(
            self.queue,
            self.on_message,
            auto_ack=True,
        )

    def on_message(self, channel, method, header, body):
        """Called when we receive a message from RabbitMQ"""
        logging.debug("New event received.........")
        logging.debug("message: ", body)
        self.handler(body)
        logging.debug("Message processing complete. Waiting for next event..........")

    def stop(self):
        if not self.closing:
            self.closing = True
            logging.info("Stopping consumer.......")
            if self.consuming:
                self.stop_consuming()
                # If connection was not closed by KeyboardInterrupt, then it should
                # stop consuming very quickly.
                if not self.consumer_stopped():
                    # Connection has been closed by KeyboardInterrupt. We must start the ioloop
                    # to complete the shutdown process.
                    self.connection.ioloop.start()
            else:
                self.connection.ioloop.stop()

    def stop_consuming(self):
        """
        Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self.channel:
            logging.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(
                self.on_cancel_ok,
                userdata=self.consumer_tag,
            )
            self.channel.basic_cancel(self.consumer_tag, cb)

    def on_cancel_ok(self, _unused_frame, userdata):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method _unused_frame: The Basic.CancelOk frame
        :param str|unicode userdata: Extra user data (consumer tag)

        """
        self.consuming = False
        logging.info('RabbitMQ acknowledged the cancellation of the consumer: %s', userdata)
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        logging.info('Closing the channel')
        self.channel.close()

    def on_channel_closed(self, channel, reason):
        logging.info("Channel %i was closed. Reason: %s", channel, reason)
        self.close_connection()

    def close_connection(self):
        self.consuming = False
        if self.connection.is_closing or self.connection.is_closed:
            logging.info('Connection is closing or already closed')
        else:
            logging.info('Closing connection.....')
            self.connection.close()

    def on_connection_closed(self, connection, reason):
        self.channel = None
        if self.closing:
            logging.info("Connection is closed. Stopping ioloop......")
            self.connection.ioloop.stop()
        else:
            logging.warning('Connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def consumer_stopped(self):
        """This will return True if consumer is stopped otherwise it will
        return False after a timeout"""
        for _ in range(5):
            if not self.consuming:
                return True
            else:
                time.sleep(1)
        return False

    def reconnect(self):
        """Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.

        """
        self.should_reconnect = True
        self.stop()


class Publisher:
    def __init__(self):
        self.server_url = os.getenv("RABBITMQ_SERVER_URL")
        self.queue = os.getenv("OUTPUT_QUEUE")

    def publish(self, payload):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.server_url,
            ))
        output_channel = connection.channel()

        output_channel.queue_declare(
            queue=self.queue,
            durable=True,
            exclusive=False,
            auto_delete=False,
        )

        output_channel.basic_publish(
            exchange='',
            routing_key=self.queue,
            body=payload,
        )
        connection.close()
