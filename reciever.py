import pika
import json

# Configuration Variables

RABBIT_MQ_SERVER_URL="localhost"
INPUT_QUEUE="test"

# Create a global channel variable to hold our channel object in
channel = None

# Step #2
def on_connected(connection):
    """Called when we are fully connected to RabbitMQ"""
    # Open a channel
    connection.channel(on_open_callback=on_channel_open)

# Step #3
def on_channel_open(new_channel):
    """Called when our channel has opened"""
    global channel
    channel = new_channel
    channel.queue_declare(queue=INPUT_QUEUE, durable=True, exclusive=False, auto_delete=False, callback=on_queue_declared)

# Step #4
def on_queue_declared(frame):
    """Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ"""
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.basic_consume(INPUT_QUEUE, handle_delivery,auto_ack=True)

# Step #5
def handle_delivery(channel, method, header, body):
    """Called when we receive a message from RabbitMQ"""
    payload = json.loads(body)
    print(payload["video"]["url"])
    # json.dumps(payload)

# Step #1: Connect to RabbitMQ using the default parameters
parameters = pika.ConnectionParameters(host=RABBIT_MQ_SERVER_URL)
connection = pika.SelectConnection(parameters, on_open_callback=on_connected)

try:
    # Loop so we can communicate with RabbitMQ
    connection.ioloop.start()
except KeyboardInterrupt:
    # Gracefully close the connection
    connection.close()
    # Loop until we're fully closed, will stop on its own
    connection.ioloop.start()
