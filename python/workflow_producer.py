#!/usr/bin/env python
# Python workflow producer client:
# sends a message to the RabbitMQ server to start a workflow
# Usage: python workflow_producer.py <workflow_event> <number_of_events> [<metadata_key=metadata_value> <metadata_key=metadata_value> ...]
# To be used together with workflow_consumer.py

import sys
import os
import pika
import logging
import time
import json
import requests
import datetime

logging.basicConfig(level=logging.CRITICAL)

class RabbitmqProducer(object):
    
    def __init__(self, workflow_event):

        # connect to RabbitMQ server: use RABBITMQ_USER_URL or default to guest/guest @ localhost
        url = os.environ.get('RABBITMQ_USER_URL', 'amqp://guest:guest@localhost/%2f')
        params = pika.URLParameters(url)
        params.socket_timeout = 5
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        
        # use default exchange, one queue per workflow
        self.queue_name = workflow_event
        self.channel.queue_declare(queue=self.queue_name, durable=True) # make queue persist server reboots

    def produce(self, message):
        '''
        Sends a message that will trigger a workflow submission.
        '''
        
        # make message persist if RabbitMQ server goes down
        self.channel.basic_publish(exchange='',
                      routing_key=self.queue_name,
                      body=message,
                      properties=pika.BasicProperties(delivery_mode=2) # make message persistent
                      )

        logging.critical("Sent workflow message %r: %r" % (workflow_event, message))
        
    def wait_for_completion(self):
        '''
        Method that waits until the number of 'unack messages is 0
        (signaling that all workflows have been completed).
        '''
        
        #url = 'http://oodt-admin:changeit@localhost:15672/api/queues/%2f/test-workflow'
        #resp = requests.get(url=url)
        #data = json.loads(resp.text)
        #print data
        #print data['messages_unacknowledged']
        #print data['messages_ready']
        
        # wait for 'Ready Messages' = 0 (i.e. all messages have been sent)
        num_msgs = -1
        while num_msgs !=0 :
            num_msgs = self.channel.queue_declare(queue=self.queue_name, durable=True, passive=True).method.message_count
            logging.critical("Number of ready messages: %s" % num_msgs)
            time.sleep(1)
            
        # then wait for the 'Unack Messages = 0' (i.e. all messages have been acknowldged)
        num_unack_messages = -1
        # FIXME
        url = 'http://oodt-admin:changeit@localhost:15672/api/queues/%2f/test-workflow'
        while num_unack_messages != 0:
            resp = requests.get(url=url)
            data = json.loads(resp.text)
            num_unack_messages = data['messages_unacknowledged']
            logging.critical("Number of unack messages: %s" % num_unack_messages)
            time.sleep(1)
        
    def close(self):
        '''
        Closes the connection to the RabbitMQ server.
        '''
        self.connection.close()
        


if __name__ == '__main__':
    ''' Command line invocation method. '''
    
    startTime = datetime.datetime.now()
    logging.critical("Start Time: %s" % startTime.strftime("%Y-%m-%d %H:%M:%S") )
 
    # parse command line arguments
    if len(sys.argv) < 3:
      raise Exception("Usage: python workflow_producer.py <workflow_event> <number_of_events> [<metadata_key=metadata_value> <metadata_key=metadata_value> ...]")
    else:
      workflow_event = sys.argv[1]
      num_events = int( sys.argv[2] )
      message = ' '.join(sys.argv[3:]) or ''


    # connect to RabbitMQ server on given queue
    rmqProducer = RabbitmqProducer(workflow_event)
    
    # send messages
    for i in range(num_events):
        rmqProducer.produce(message)
        
    # wait a little for messages to be logged
    time.sleep(3)
    # then wait for all messages to be acknowledged
    rmqProducer.wait_for_completion()
    
    # shut down
    rmqProducer.close()
    
    stopTime = datetime.datetime.now()
    logging.critical("Stop Time: %s" % stopTime.strftime("%Y-%m-%d %H:%M:%S") )
    logging.critical("Elapsed Time: %s secs" % (stopTime-startTime).seconds )
