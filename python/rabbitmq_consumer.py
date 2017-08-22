#!/usr/bin/env python

# Usage: # Usage: python rabbitmq_consumer.py <workflow_event> <max_number_of_concurrent_workflows>

import sys
import os
import pika
import time
import threading
import logging
import json
from workflow_client import WorkflowManagerClient

logging.basicConfig(level=logging.DEBUG,
                    format='%(levelname)s %(filename)s %(funcName)s: %(message)s')


class RabbitmqConsumer(threading.Thread):
    '''
    RabbitMQ message consumer that "pulls" messages from the RabbitMQ message broker.
    After a message is pulled, the consumer will try to submit a workflow to the Workflow Manager:
    if the workflow is submitted succesfully, the message is acknoledged; if not, the message is
    rejected (and will be re-pulled by another RabbitmMQ consumer).
    This class will only pull a given maximum number of messages at a time, then waiting for the
    Workflow Manager load to drop below that threshold before pulling more messages.
    '''

    # time interval in seconds before pulling the next message
    TIME_INTERVAL = 1


    def __init__(self, workflow_event, wmgrClient,
                 group=None, target=None, name=None, verbose=None):  # Thread parent class arguments

        # initialize Thread
        threading.Thread.__init__(
            self, group=group, target=target, name=name, verbose=verbose)

        # workflow manager client
        self.wmgrClient = wmgrClient

        # RABBITMQ_USER_URL (defaults to guest/guest @ localhost)
        self.rabbitmqUrl = os.environ.get(
            'RABBITMQ_USER_URL', 'amqp://guest:guest@localhost/%2f')

        # rabbitmq queue name = OODT workflow event
        self.queue_name = workflow_event

        # connections to the EabbitMQ server
        self.connection = None
        self.channel = None


    def connect(self):
        '''Method to open a connection to the RabbitMQ server.'''

        # connect to RabbitMQ server
        if self.connection is None or self.connection.is_closed:
            params = pika.URLParameters(self.rabbitmqUrl)
            params.socket_timeout = 10
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
            # make queue persist server reboots
            self.channel.queue_declare(queue=self.queue_name, durable=True)


    def disconnect(self):
        '''Method to close the connection to the RabbitMQ server.'''

        if self.connection.is_open:
            self.connection.close()


    def run(self):
        '''Method to pull messages from the RabbitMQ server
           and submit workflows to the Workflow Manager.'''

        logging.info("Pulling messages from RabbitMQ server...")

        while True:
            try:  # program terminated

                # wait for Workflow Manager to be ready for the next workflow
                logging.debug("Waiting for WM Client to be ready...")
                if self.wmgrClient.isReady():

                    logging.debug("Trying to pull next message from queue: %s" % self.queue_name)
                    try:
                        # open connection if necessary
                        self.connect()
                        method_frame, header_frame, body = self.channel.basic_get(self.queue_name)

                        if method_frame:

                            logging.debug("Message found:")
                            logging.debug(method_frame)
                            logging.debug(header_frame)
                            logging.debug(body)

                            # submit workflow, then block to wait for its completion
                            metadata = json.loads(body)
                            logging.info("RMQ client: submitting workflow with metadata: %s" % metadata)
                            status = self.wmgrClient.submitWorkflow(metadata)
                            logging.info('RMQ client: worfklow submission status: %s' % status)
                            
                            if status:
                                # workflow submitted succesfully --> send message acknowledgment
                                self.channel.basic_ack(method_frame.delivery_tag)
                            else:
                                # workflow submission resuted in error --> send message rejection, message will be re-queued on server
                                logging.warn("RMQ client: sending NACK for worflow with metadata: %s" % metadata)
                                self.channel.basic_nack(method_frame.delivery_tag)             

                        else:
                            # leave the connection open for the next pull
                            logging.debug('No message returned')
                            
                    except (pika.exceptions.ConnectionClosed, 
                            pika.exceptions.IncompatibleProtocolError,
                            pika.exceptions.ProbableAuthenticationError) as e:

                        # do nothing, wait for next attempt
                        logging.debug("Connection error, will retry...")
                        logging.warn(e)

                # wait till next check
                logging.debug("RMQ client waiting...")
                time.sleep(self.TIME_INTERVAL)

            # but stop if ^C is issued
            except (KeyboardInterrupt, SystemExit):
                logging.info("Stopping rabbitmq client...")
                self.disconnect()
                break


if __name__ == '__main__':
    ''' Command line invocation method. '''

    # parse command line argument
    if len(sys.argv) < 3:
        raise Exception(
            "Usage: python rabbitmq_client.py <workflow_event> <max_num_running_workflow_instances>")
    else:
        workflow_event = sys.argv[1]
        max_num_running_workflow_instances = int(sys.argv[2])

    # instantiate RabbitMQ client
    # the collaborating WorkflowClient will contact the Workflow manager at $PROXIED_WORKFLOW_URL, or $WORKFLOW_URL, or http://localhost:9001
    wmgrClient = WorkflowManagerClient(
        workflow_event, 
        workflowManagerUrl=os.getenv("PROXIED_WORKFLOW_URL", os.getenv("WORKFLOW_URL", "http://localhost:9001")),
        max_num_running_workflow_instances=max_num_running_workflow_instances)

    # instantiate RabbitMQ client
    rmqClient = RabbitmqConsumer(workflow_event, wmgrClient)

    # start listening for workflow events
    rmqClient.start()

    logging.info('Waiting for workflow events. To exit press CTRL+Z')
