import pika
import json

RABBIT_MQ_SERVER="localhost"
OUTPUT_QUEUE="test"

# payload
meta = '{"scrape_id":"190sdflasdf902130asdf","scrape_timestamp":"2021-10-10 10:30PM","source":{"id":"1890203","name":"Bangla Vision","type":"Youtube"},"video":{"title":"Banglavision News","publish_date":"2021-10-08 02:00PM","duration":"18m54s","url":"https://youtu.be/2ia0aLimZJ4"}}'

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBIT_MQ_SERVER))
channel = connection.channel()

channel.queue_declare(queue=OUTPUT_QUEUE, durable=True, exclusive=False, auto_delete=False)

# payload = json.loads(meta)
channel.basic_publish(exchange='', routing_key=OUTPUT_QUEUE, body=meta)
print(" [x] Sent 'Hello World!'")
connection.close()
