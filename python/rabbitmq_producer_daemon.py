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
logging.basicConfig(level=logging.INFO)

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
        
        self._amqp_url = amqp_url
        self._connection = None
        self._channel = None
        
        # flag to stop the daemon
        self._stopped = False 
        
        # unique producer identifier
        self._producer_id = str(uuid.uuid4())  

        # message counters
        self._message_number = 0
        self._acked = 0
        self._nacked = 0
        
        # list of queues==events that have been initialized
        self._queues = [] 

                
    def _connect(self):
        '''Opens connection to RabbitMQ server.'''
        
        if self._connection is None or self._connection.is_closed:
            
            logging.info("RabbitmqProducerDaemon: connecting to: %s" % self._amqp_url)
        
            self._connection = pika.BlockingConnection(pika.URLParameters(self._amqp_url)) 
            self._channel = self._connection.channel()
            
            self._channel.exchange_declare(exchange=EXCHANGE_NAME, 
                                           type=EXCHANGE_TYPE,
                                           durable=True)  # survive server reboots
            
            # enable delivery confirmation
            self._channel.confirm_delivery()
            
            # reset all queues
            self._queues = [] 
            
    def _setup_queue(self, event_name):
        '''Sets up an exchange/queue pair.'''
        
        if event_name not in self._queues:
            
            self._channel.queue_declare(queue=event_name, durable=True) # make queue persist server reboots
            
            # bind the exchange to the queue with a routing key equal to the queue name itself
            self._channel.queue_bind(exchange=EXCHANGE_NAME,
                                     queue=event_name,
                                     routing_key=event_name) 
            
            self._queues.append(event_name)

        
            
    
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
        '''Publishes a single message to a queue.'''
        
        if self._stopped:
            return
        
        # re-connect, if necessary
        self._connect()
        
        # declare the queue, if not done already
        self._setup_queue(event_name)
                
        # send the message
        self._message_number += 1
        properties = pika.BasicProperties(app_id=self._producer_id,
                                          content_type='application/json',
                                          delivery_mode=2,       # make message persistent
                                          headers=metadata)

        routing_key = event_name
        status = self._channel.basic_publish(EXCHANGE_NAME, routing_key,
                                    # transforms dictionary into string
                                    json.dumps(metadata, ensure_ascii=False),
                                    properties)
        if status:
            self._acked += 1   # message acknowledged
        else:  
            self._nacked += 1  # message NOT acknowledged
        
        logging.info('Published message to workflow: %s with metadata: %s, acknowledgment status=%s' % (event_name, metadata, status))
        logging.debug('Total number of messages sent=%s, acknowledged=%s, not acknowledged=%s' % (self._message_number, self._acked, self._nacked))
        
        
    def stop(self):
        '''Method to stop the daemon.'''
        
        logging.info("RabbitmqProducerDaemon: stopping.")
        
        self._stopped = True
        self._disconnect()
        
    def _disconnect(self):
        '''Closes connection to the RabbitMQ server.'''
        
        if self._channel:
            self._channel.close()
            
        if self._connection.is_open:
            self._connection.close()
            
        logging.info("RabbitmqProducerDaemon disconnected.")
                

if __name__ == '__main__':
    
    event_name = "test-workflow"
    metadata = { "Project":"123", "Dataset":"abc", "Run":"1"}
    
    rmqp = RabbitmqProducerDaemon(RABBITMQ_USER_URL)
    rmqp.start()
    
    rmqp.publish_message(event_name, metadata)
    rmqp.publish_message(event_name, metadata)
    rmqp.publish_message(event_name, metadata)
    
    rmqp.stop()