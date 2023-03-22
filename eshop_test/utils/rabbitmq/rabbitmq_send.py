import pika
import uuid
import json


# To test RabbitMQ use the following command:
# docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.11-management


class RabbitMQ:
    def __init__(self, host='localhost'):
        self.connection = None
        self.channel = None
        self.host = host

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.connection.close()

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.channel = self.connection.channel()

    def declare_queue(self, queue):
        self.channel.queue_declare(queue=queue)

    def close(self):
        self.connection.close()

    def publish(self, exchange, routing_key, body):
        self.channel.basic_publish(exchange=exchange,
                                   routing_key=routing_key,
                                   body=body)
        print(f"[{routing_key}] Sent '{body}'")

    def consume(self, queue, callback):
        a = self.channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()
        return a

    def purge(self,queue):
        self.channel.queue_purge(queue=queue)


if __name__ == '__main__':
    body = {
        "OrderId": 1,
        "Id": str(uuid.uuid4()),
        "CreationDate": "2023-03-05T15:33:18.1376971Z"

        }

    with RabbitMQ() as mq:
        mq.publish(exchange='eshop_event_bus',
                  routing_key='OrderPaymentSucceededIntegrationEvent',
                  body=json.dumps(body))

        #queue='Ordering.signalrhub'
        #mq.purge(queue)

        # mq.publish(exchange='eshop_event_bus',
        #            routing_key='UserCheckoutAcceptedIntegrationEvent',
        #            body=json.dumps(body))