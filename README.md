# py-modules

Various Python helper modules. Currently, it has helper module for interacting with the following tools:

- RabbitMQ
- Minio

## Usage

### RabbitMQ

**Start Consumer:**

```python
import rabbitmq


def event_handler(message):
    print("Received: ", message)


consumer = rabbitmq.Consumer(handler=event_handler)
try:
    consumer.start()
except KeyboardInterrupt:
    consumer.stop()
```

**Publish a Message:**

```python
import rabbitmq

publisher = rabbitmq.Publisher()
msg = 'Hello world!'
try:
    publisher.publish(msg)
except Exception as e:
    raise Exception("Failed to publish message.") from e
```

### Minio

**Upload a file:**

```python
import minio_client

bucket = "my-bucket"
filename = "/my/sample/file.txt"

minio = minio_client.MinioClient(bucket)
minio.upload_file(filename)
```

**Download a file:**

```python
import minio_client

bucket = "my-bucket"
filename = "/my/sample/file.txt"
output_filename = "/my/sample/output/file.txt"

minio = minio_client.MinioClient(bucket)
minio.download_file(filename, output_filename)
```

## Testing

**Start Minio and RabbitMQ Server:**

```bash
docker-compose up
```

**Run RabbitMQ Tests:**

```shell
python3 -m unittest -v test_rabbitmq.py
```

**Run Minio Server Tests:**

```shell
python3 -m unittest -v test_minio_client.py
```
