from consumer_interface import mqConsumerInterface
import pika
from pika import BlockingConnection, ConnectionParameters


class mqConsumer(mqConsumerInterface):
    
    
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.connection = None
        self.setupRMQConnection()

        pass
    
    def setupRMQConnection(self) -> None:
        '''
        setupRMQConnection Function: Establish connection to the RabbitMQ service,
        declare a queue and exchange, 
        bind the binding key to the queue on the exchange and finally set up a callback function for receiving messages
        '''
        self.connection = BlockingConnection(ConnectionParameters(host='rabbitmq'))

        # Establish Channel
        channel = self.connection.channel()

        # Create Queue if not already present
        channel.queue_declare(queue=self.queue_name)

        # Create the exchange if not already present
        channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

        # Bind Binding Key to Queue on the exchange
        channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=self.binding_key)

        # Set-up Callback function for receiving messages
        channel.basic_consume(queue=self.queue_name, on_message_callback=self.on_message_callback)
        pass

    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        '''
        # Acknowledge message

        #Print message (The message is contained in the body parameter variable)
        '''
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        print(f"Received message: {body.decode()}")
        pass
    
    def startConsuming(self) -> None:
        '''
        Print " [*] Waiting for messages. To exit press CTRL+C"

        Start consuming messages
        '''
        print(" [*] Waiting for messages. To exit press CTRL+C")
        self.connection.process_data_events(time_limit=None)
        pass
    
    def __del__(self) -> None:
        '''
        Print "Closing RMQ connection on destruction"
        
        Close Channel

        Close Connection
        '''
        print("Closing RMQ connection on destruction")
        if self.connection and not self.connection.is_closed:
            self.connection.close()
        pass
  
