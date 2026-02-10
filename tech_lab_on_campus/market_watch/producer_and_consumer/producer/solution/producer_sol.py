from producer_interface import mqProducerInterface
import pika

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.connection = None
        self.setupRMQConnection()
    
    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))

        # Establish Channel
        self.channel = self.connection.channel()

        # Create the exchange if not already present
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

    def publishOrder(self, message: str) -> None:
        # Basic Publish to Exchange
        self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_key, body=message)

        # Close Channel
        self.channel.close()

        # Close Connection
        self.connection.close()
