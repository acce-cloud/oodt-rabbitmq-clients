#!/usr/bin/env python


# Usage: # Usage: python rabbitmq_pushed_client.py <workflow_event> <number_of_clients>

import sys
import os
import pika
import time
import threading
import logging
import json
from workflow_client import WorkflowManagerClient

logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)-10s) %(message)s')


class RabbitmqClient(threading.Thread):
    '''
    Superclass of RabbitMQ clients that act in push or pull mode.
    '''

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
        raise NotImplementedError


class RabbitmqPushClient(RabbitmqClient):
    '''
    RabbitMQ client to which workflow messages are pushed to by the RabbitMQ server.
    Upon receiving a new message, the client submits a workflow to the OODT workflow manager and blocks until its completion.
    The message acknowledgment is sent to the server after the workflow completes.
    Each client instance can submit and monitor only one workflow at a time (in its own separate thread).
    '''

    def __init__(self, workflow_event, wmgrClient,
                 group=None, target=None, name=None, verbose=None):

        RabbitmqClient.__init__(self, workflow_event, wmgrClient,
                                group=group, target=target, name=name, verbose=verbose)

    def run(self):
        '''Method to listen for messages from the RabbitMQ server.'''

        logging.debug("Listening for messages from RabbitMQ server...")

        # open connection
        self.connect()

        # process 1 message at a time from this queue
        self.channel.basic_qos(prefetch_count=1)

        self.channel.basic_consume(
            self._callback, queue=self.queue_name)  # no_ack=False
        self.channel.start_consuming()

    def _callback(self, ch, method, properties, body):
        '''Callback method invoked when a RabbitMQ message is received.'''

        # parse message body into metadata dictionary
        metadata = json.loads(body)
        logging.info("Submitting workflow")

        # submit workflow, then wait for its completeion
        logging.info("Received message: %r: %r, submitting workflow..." %
                     (method.routing_key, metadata))
        status = self.wmgrClient.executeWorkflow(metadata)
        logging.info('Worfklow ended with status: %s' % status)

        # send acknowledgment to RabbitMQ server
        ch.basic_ack(delivery_tag=method.delivery_tag)


class RabbitmqPullClient(RabbitmqClient):
    '''
    RabbitMQ client that pulls messages from the RabbitMQ server.
    Upon fetching a new message, the client immediately acknowledges the message, 
    then submits a workflow to the OODT workflow manager and blocks until its completion.
    Each client instance can submit and monitor only one workflow at a time (in its own separate thread).
    '''

    # time interval in seconds before pulling the next message
    TIME_INTERVAL = 5

    def __init__(self, workflow_event, wmgrClient,
                 group=None, target=None, name=None, verbose=None):

        RabbitmqClient.__init__(self, workflow_event, wmgrClient,
                                group=group, target=target, name=name, verbose=verbose)

    def run(self):
        '''Method to pull messages from the RabbitMQ server.'''

        logging.info("Pulling messages from RabbitMQ server...")

        while True:
            try:  # program terminated

                # wait for Workflow Manager to be ready for the next workflow
                if self.wmgrClient.isReady():

                    logging.debug("Trying to pull next message from queue: %s" % self.queue_name)
                    try:
                        # open connection if necessary
                        self.connect()
                        method_frame, header_frame, body = self.channel.basic_get(self.queue_name)

                        if method_frame:

                            logging.info("Message found:")
                            logging.info(method_frame)
                            logging.info(header_frame)
                            logging.info(body)

                            # send message acknowledgment
                            self.channel.basic_ack(method_frame.delivery_tag)


                            # submit workflow, then block to wait for its completion
                            metadata = json.loads(body)
                            logging.info("RMQ client: submitting workflow with metadata: %s" % metadata)
                            status = self.wmgrClient.submitWorkflow(metadata)
                            logging.info('Worfklow submission status: %s' % status)
                            
                            if status:
                                # workflow submitted succesfully --> send message acknowledgment
                                self.channel.basic_ack(method_frame.delivery_tag)
                            else:
                                # workflow submission resuted in error --> send message rejection
                                logging.warn("RMQ client: sending NACK for worflow with metadata: %s" % metadata)
                                self.channel.basic_nack(method_frame.delivery_tag)             

                        else:
                            # leave the connection open for the next pull
                            logging.debug('No message returned')
                            
                        # disconnect
                        self.disconnect()

                    except (pika.exceptions.ConnectionClosed, pika.exceptions.ProbableAuthenticationError) as e:
                        # do nothing, wqit for next attempt
                        logging.info("Connection error, will retry...")
                        logging.warn(e)
                        

                # wait till next check
                logging.debug("RMQ client waiting...")
                time.sleep(self.TIME_INTERVAL)

            # but stop if ^C is issued
            except (KeyboardInterrupt, SystemExit):
                logging.info("Stopping rabbitmq client...")
                self.disconnect()


if __name__ == '__main__':
    ''' Command line invocation method. '''

    # parse command line argument
    if len(sys.argv) < 4:
        raise Exception(
            "Usage: python rabbitmq_client.py <push/pull> <workflow_event> <max_num_running_workflow_instances>")
    else:
        push_or_pull = sys.argv[1]
        workflow_event = sys.argv[2]
        max_num_running_workflow_instances = int(sys.argv[3])

    # instantiate RabbitMQ client
    wmgrClient = WorkflowManagerClient(
        workflow_event, max_num_running_workflow_instances=max_num_running_workflow_instances)

    # instantiate pull/push RabbitMQ client
    if push_or_pull == 'push':
        rmqClient = RabbitmqPushClient(workflow_event, wmgrClient)
    elif push_or_pull == 'pull':
        rmqClient = RabbitmqPullClient(workflow_event, wmgrClient)
    else:
        raise Exception("Unrecognized push/pull argument: %s" % push_or_pull)

    # start listening for workflow events
    rmqClient.start()

    logging.info('Waiting for workflow events. To exit press CTRL+Z')
