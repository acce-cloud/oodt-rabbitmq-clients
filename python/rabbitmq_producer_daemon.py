'''
RabbitMQ producer that runs as a daemon.
It keeps accepting requests for publishing messages to the RabbitMQ server untill it is stopped.

@author: cinquini
'''

import pika
import uuid
import json
import logging
import threading
import time

# Set up logging
logging.basicConfig(level=logging.DEBUG)

RABBITMQ_USER_URL='amqp://oodt-user:changeit@192.168.99.100/%2f' # FIXME
EXCHANGE_NAME = 'oodt-exchange'
EXCHANGE_TYPE = 'direct'
PUBLISH_INTERVAL = 0.1
TIME_INTERVAL = 1 # thread waiting time

class RabbitmqProducerDaemon(threading.Thread):
    

    def __init__(self, amqp_url,
                 group=None, target=None, name=None, verbose=None):  # Thread parent class arguments):
        
        # initialize Thread
        threading.Thread.__init__(self, group=group, target=target, name=name, verbose=verbose)
        
        self._message_number = 0
        self._amqp_url = amqp_url
        self._producer_id = str(uuid.uuid4())  # unique producer identifier
        self._connection = None
        self._channel = None
        self._stopped = False # flag to stop the daemon
        
        # connect to the server
        self._connect()
        
    def _connect(self):
        '''Opens connection to RabbitMQ server.'''
        
        self._connection = pika.BlockingConnection(pika.URLParameters(self._amqp_url )) 
        self._channel = self._connection.channel()
        
        self._channel.exchange_declare(exchange=EXCHANGE_NAME, 
                                       type=EXCHANGE_TYPE,
                                       durable=True)  # survive server reboots

    
    def run(self):
        '''Daemon method - keeps running until stopped.'''
        
        logging.info("RabbitmqProducerDaemon: starting...")
        
        while not self._stopped:
            try: 
                time.sleep(TIME_INTERVAL)

            # program terminated by ^C
            except (KeyboardInterrupt, SystemExit):
                logging.info("RabbitmqProducerDaemon: stopping.")
                self._disconnect()
                break
    
    def publish_message(self, event_name, metadata):
                
        # make queue persist server reboots
        self._channel.queue_declare(queue=event_name, durable=True)
        
        self._channel.queue_bind(exchange=EXCHANGE_NAME,
                           queue=event_name,
                           routing_key=event_name)
        
        self._message_number += 1
        properties = pika.BasicProperties(app_id=self._producer_id,
                                          content_type='application/json',
                                          delivery_mode=2,       # make message persistent
                                          headers=metadata)

        routing_key = event_name
        self._channel.basic_publish(EXCHANGE_NAME, routing_key,
                                    # transforms dictionary into string
                                    json.dumps(metadata, ensure_ascii=False),
                                    properties)

        #self._deliveries.append(self._message_number)
        logging.critical('Published message to workflow: %s with metadata: %s' % (event_name, metadata))
        
        
    def stop(self):
        '''Method to stop the daemon.'''
        
        self._stopped = True
        self._disconnect()
        
    def _disconnect(self):
        '''Closes connection to the RabbitMQ server.'''
        
        if self._connection.is_open:
            self._connection.close()
                

if __name__ == '__main__':
    
    event_name = "test-workflow"
    metadata = { "Project":"123", "Dataset":"abc", "Run":"1"}
    
    rmqp = RabbitmqProducerDaemon(RABBITMQ_USER_URL)
    rmqp.start()
    
    rmqp.publish_message(event_name, metadata)
    rmqp.publish_message(event_name, metadata)
    rmqp.publish_message(event_name, metadata)
    
    rmqp.stop()