# py-rmq

Python template for interacting with a  RabbitMQ Server


## Usage

**Install dependencies:**

At first, run the following command in this repo root directory to install necessary python libraries.

```bash
pip3 install -r ./requirements.tx
```

**Start RabbitMQ Server:**

```bash
docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.9-management
```

**Start RabbitMQ Receiver:**

```bash
python3 ./reciever.py
```

**Send Message:**

```bash
python3 ./sender.py
```
