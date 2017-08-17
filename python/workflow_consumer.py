#!/usr/bin/env python

# Python workflow consumer client:
# receives messages from the RabbitMQ server to start OODT workflows
# Usage: # Usage: python workflow_consumer.py <workflow_event> <number_of_concurrent_workflows_per_engine>
# To be used together with workflow_producer.py

import sys
import os
import pika
import xmlrpclib
import time
import threading
import logging
from workflow_client import WorkflowManagerClient

logging.basicConfig(level=logging.INFO, format='(%(threadName)-10s) %(message)s')



class RabbitmqConsumer(threading.Thread):
    '''
    Python client that consumes messages from the RabbitMQ server,
    and triggers execution of workflows through the WorkflowManagerClient.
    This class listens for messages in a separate thread.
    '''
    
    def __init__(self, workflow_event, wmgrClient, 
                 group=None, target=None, name=None, verbose=None): # Thread parent class arguments
        
        # initialize Thread
        threading.Thread.__init__(self, group=group, target=target, name=name, verbose=verbose)
        
        # workflow manager client
        self.wmgrClient = wmgrClient
        
        # RABBITMQ_USER_URL (defaults to guest/guest @ localhost)
        rabbitmqUrl = os.environ.get('RABBITMQ_USER_URL', 'amqp://guest:guest@localhost/%2f')
        
        # connect to RabbitMQ server
        params = pika.URLParameters(rabbitmqUrl)
        params.socket_timeout = 5
        connection = pika.BlockingConnection(params)
        self.channel = connection.channel()
                                
        # connect to queue for given workflow
        self.queue_name = workflow_event
        self.channel.queue_declare(queue=self.queue_name, durable=True) # make queue persist server reboots
                 
    def run(self):
        logging.debug("Listening for messages...")
        self._consume()       
        
    def _consume(self):
        '''Method to listen for messages from the RabbitMQ server.'''
        
        # process 1 message at a time from this queue
        self.channel.basic_qos(prefetch_count=1)
        
        self.channel.basic_consume(self._callback, queue=self.queue_name) # no_ack=False
        self.channel.start_consuming()
        
        
    def _callback(self, ch, method, properties, body):
        '''Callback method invoked when a RabbitMQ message is received.'''

        # parse message body into metadata dictionary
        # from: 'Dataset=abc Project=123'
        # to: { 'Dataset':'abc', 'Project': '123' }
        metadata = dict(word.split('=') for word in body.split())
                
        # submit workflow, then wait for its completeion
        logging.info("Received message: %r: %r, submitting workflow..." % (method.routing_key, metadata))
        #time.sleep(10)
        status = self.wmgrClient.executeWorkflow(metadata)      
        logging.info('Worfklow ended with status: %s' % status)
        
        # send acknowledgment to RabbitMQ server
        ch.basic_ack(delivery_tag = method.delivery_tag)


if __name__ == '__main__':
    ''' Command line invocation method. '''
    
    # parse command line argument
    if len(sys.argv) < 3:
      raise Exception("Usage: python workflow_consumer.py <workflow_event> <number_of_concurrent_workflows_per_engine>")
    else:
      workflow_event = sys.argv[1]
      num_workflow_clients = int(sys.argv[2])
      
    # instantiate N RabbitMQ clients
    for i in range(num_workflow_clients):
        
        # instantiate Workflow Manager client
        # IMPORTANT: xmlrpclib is NOT thread safe in Python 2.7
        # so must create one WorkflowManagerClient to be used in each thread
        wmgrClient = WorkflowManagerClient(workflow_event)
        
        rmqConsumer = RabbitmqConsumer(workflow_event, wmgrClient)
    
        # start listening for workflow events
        rmqConsumer.start()
    
    logging.info('Waiting for workflow events. To exit press CTRL+Z')
    