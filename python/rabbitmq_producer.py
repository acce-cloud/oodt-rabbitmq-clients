# -*- coding: utf-8 -*-
# Adapted from: http://pika.readthedocs.io/en/0.10.0/examples/asynchronous_publisher_example.html
#
# Features:
# o Requests delivery confirmation from RabbitMQ server 
#   and keeps tracks of which messages have been acknowledged or not acknowledged
# o Reconnects if connection to RabbitMQ servers goes down for any reason
# o Shuts down if RabbitMQ server closes the channel
# 
# Connection parameters are specified through the environmental variables:
# o RABBITMQ_USER_URL (to send messages)
# o RABBITMQ_ADMIN_URL (to monitor message delivery)
#
# Usage: python rabbitmq_producer.py <workflow_event> <number_of_events> [<metadata_key=metadata_value> <metadata_key=metadata_value> ...]
# To be used together with rabbitmq_consumer.py

import logging
import pika
import json
import os
import sys
import datetime
import requests
import time
import uuid

#LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s')
LOG_FORMAT = '%(levelname)s: %(message)s'
LOGGER = logging.getLogger(__name__)
LOG_FILE = "rabbitmq_producer.log" # in current directory


class RabbitmqProducer(object):
    """This is an example publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.

    """
    EXCHANGE = 'oodt-exchange' 
    EXCHANGE_TYPE = 'direct'
    PUBLISH_INTERVAL = 0.1
    PRODUCER_ID = str(uuid.uuid4()) # unique producer identifer
    

    def __init__(self, amqp_url, workflow_event, num_messages, msg_dict):
        """Setup the example publisher object, passing in the URL we will use
        to connect to RabbitMQ.

        :param str amqp_url: The URL for connecting to RabbitMQ

        """
        self._connection = None
        self._channel = None
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._stopping = False
        self._url = amqp_url
        self._closing = False
        self._queue = workflow_event
        self._routing_key = workflow_event
        self._num_messages = num_messages
        self._msg_dict = msg_dict


    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika. If you want the reconnection to work, make
        sure you set stop_ioloop_on_close to False, which is not the default
        behavior of this adapter.

        :rtype: pika.SelectConnection

        """
        LOGGER.info('Connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :type unused_connection: pika.SelectConnection

        """
        LOGGER.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        LOGGER.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        # Create a new connection
        self._connection = self.connect()

        # There is now a new connection, needs a new ioloop to run
        self._connection.ioloop.start()

    def open_channel(self):
        """This method will open a new channel with RabbitMQ by issuing the
        Channel.Open RPC command. When RabbitMQ confirms the channel is open
        by sending the Channel.OpenOK RPC reply, the on_channel_open method
        will be invoked.

        """
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        LOGGER.warning('Channel was closed: (%s) %s', reply_code, reply_text)
        if not self._closing:
            self._connection.close()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOGGER.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(callback=self.on_exchange_declareok,
                                       exchange=exchange_name,
                                       exchange_type=self.EXCHANGE_TYPE,
                                       durable=True) # survive server reboots

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        LOGGER.info('Exchange declared')
        self.setup_queue(self._queue)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        LOGGER.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue=queue_name, durable=True) # make queue persist server reboots

    def on_queue_declareok(self, method_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        LOGGER.info('Binding %s to %s with %s',
                    self.EXCHANGE, self._queue, self._routing_key)
        self._channel.queue_bind(self.on_bindok, self._queue,
                                 self.EXCHANGE, self._routing_key)

    def on_bindok(self, unused_frame):
        """This method is invoked by pika when it receives the Queue.BindOk
        response from RabbitMQ. Since we know we're now setup and bound, it's
        time to start publishing."""
        LOGGER.info('Queue bound')
        self.start_publishing()

    def start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ.

        """
        LOGGER.info('Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()
        self.schedule_next_message()
        

    def enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.

        """
        LOGGER.info('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOGGER.info('Received %s for delivery tag: %i',
                    confirmation_type,
                    method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        LOGGER.info('Published %i messages, %i have yet to be confirmed, '
                    '%i were acked and %i were nacked',
                    self._message_number, len(self._deliveries),
                    self._acked, self._nacked)
        
        # close connection and channel all messages have been delivered successfully
        if self._acked == self._num_messages:
            LOGGER.info("RabbitMQ producer shutting down...")
            self.stop()

    def schedule_next_message(self):
        """If we are not closing our connection to RabbitMQ, schedule another
        message to be delivered in PUBLISH_INTERVAL seconds.

        """
        if self._stopping:
            return
        LOGGER.info('Scheduling next message for %0.1f seconds',
                    self.PUBLISH_INTERVAL)
        self._connection.add_timeout(self.PUBLISH_INTERVAL,
                                     self.publish_message)

    def publish_message(self):
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        on_delivery_confirmations method.

        Once the message has been sent, schedule another message to be sent.
        The main reason I put scheduling in was just so you can get a good idea
        of how the process is flowing by slowing down and speeding up the
        delivery intervals by changing the PUBLISH_INTERVAL constant in the
        class.

        """
        if self._stopping:
            return

        self._message_number += 1
                
        properties = pika.BasicProperties(app_id=self.PRODUCER_ID,
                                          content_type='application/json',
                                          delivery_mode=2,       # make message persistent
                                          headers=self._msg_dict)

        self._channel.basic_publish(self.EXCHANGE, self._routing_key,
                                    # transforms dictionary into string
                                    json.dumps(self._msg_dict, ensure_ascii=False),
                                    properties)
        
        self._deliveries.append(self._message_number)
        LOGGER.critical('Published message to workflow: %s with metadata: %s' % (self._routing_key, self._msg_dict))
        
        # stop publishing after num_messages
        if self._message_number < self._num_messages:
            self.schedule_next_message()
    

    def close_channel(self):
        """Invoke this command to close the channel with RabbitMQ by sending
        the Channel.Close RPC command.

        """
        LOGGER.info('Closing the channel')
        if self._channel:
            self._channel.close()

    def run(self):
        """Run the example code by connecting and then starting the IOLoop.

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Stop the example by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published. The IOLoop is started because this method is
        invoked by the Try/Catch below when KeyboardInterrupt is caught.
        Starting the IOLoop again will allow the publisher to cleanly
        disconnect from RabbitMQ.

        """
        LOGGER.info('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()
        self._connection.ioloop.start()
        LOGGER.info('Stopped')


    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        LOGGER.info('Closing connection')
        self._closing = True
        self._connection.close()
        
def wait_for_queue(queue_name, delay_secs=0):
    '''
    Method that waits until the number of 'ready' messages and 'unacked' messages in a specific queue is 0
    (signaling that all workflows have been completed).
    Use ^C to stop waiting before all messages have been processed.
    '''
    
    LOGGER.critical("Waiting for all messages to be processed in queue: %s" % queue_name)
    time.sleep(delay_secs) # wait for queue to be ready
            
    num_messages = -1
    num_ready_messages = -1
    num_unack_messages = -1
    
    # must connect to RabbitMQ server with administrator privileges
    # RABBITMQ_ADMIN_URL=http://oodt-admin:changeit@localhost:15672
    url = os.environ.get('RABBITMQ_ADMIN_URL', 'http://guest:guest@localhost:15672') + "/api/queues/%2f/" + queue_name
    
    while num_messages != 0:
        try:
            resp = requests.get(url=url)
            data = json.loads(resp.text)
            num_messages = data['messages']
            num_ready_messages = data['messages_ready']
            num_unack_messages = data['messages_unacknowledged']
            logging.critical("Number of messages: ready=%s unacked= %s total=%s" % 
                             (num_ready_messages, num_unack_messages, num_messages))
            time.sleep(1) 
        except KeyboardInterrupt:
            LOGGER.info("Breaking out of wait mode...")
            break
        

def wait_for_queues(delay_secs=0, sleep_secs=1):
    '''
    Method that waits until the number of 'ready' messages and 'unacked' messages in all the queues is 0
    (signaling that all workflows have been completed).
    Use ^C to stop waiting before all messages have been processed.
    '''
    
    start_time = datetime.datetime.now()
    LOGGER.critical("Waiting for all messages to be processed in all queues...")
    time.sleep(delay_secs) # wait for queue to be ready
            
    num_messages = -1
    num_ready_messages = -1
    num_unack_messages = -1
    
    # must connect to RabbitMQ server with administrator privileges
    # RABBITMQ_ADMIN_URL=http://oodt-admin:changeit@localhost:15672
    url = os.environ.get('RABBITMQ_ADMIN_URL', 'http://guest:guest@localhost:15672') + "/api/queues"
    
    # open log file (override existing)
    with open(LOG_FILE, 'w') as log_file:
      # header line
      log_file.write('Elapsed time\tQueue\tNumReadyMessages\tNumUnackMessages\tNumMessages\n')

      while num_messages != 0:
        try:
            resp = requests.get(url=url)
            all_data = json.loads(resp.text)
            this_time = datetime.datetime.now()
            
            # loop over queues
            for queue_data in all_data:
                queue_name = queue_data['name']
                num_messages = queue_data['messages_persistent']
                num_ready_messages = queue_data['messages_ready']
                num_unack_messages = queue_data['messages_unacknowledged']
                
                # write out to log file
                elapsed_time = (this_time - start_time).total_seconds()
                logging.critical("Elapsed time=%s queue=%s number of messages: ready=%s unacked= %s total=%s" %
                                 (elapsed_time, queue_name, num_ready_messages, num_unack_messages, num_messages))
                log_file.write("%s\t%s\t%s\t%s\t%s\n" % (elapsed_time, queue_name, num_ready_messages, num_unack_messages, num_messages))

                # wait for this queue, skip reamining
                if num_messages > 0:
                   time.sleep(sleep_secs)
                   break # skip remaining queues, query again all queues for updated status
                
        except KeyboardInterrupt:
            LOGGER.info("Breaking out of wait mode...")
            break
        

def publish_messages(msg_queue, num_msgs, msg_dict):
    
    logging.basicConfig(level=logging.CRITICAL, format=LOG_FORMAT)
        
    # RABBITMQ_USER_URL (defaults to guest/guest @ localhost)
    # connect to virtual host "/" (%2F)
    rabbitmqUrl = os.environ.get('RABBITMQ_USER_URL', 'amqp://guest:guest@localhost/%2f')

    # instantiate producer
    rmqProducer = RabbitmqProducer(rabbitmqUrl + '?connection_attempts=3&heartbeat_interval=3600', 
                                        msg_queue, num_msgs, msg_dict)
    
    # publish N messages
    rmqProducer.run()
    
    # wait for RabbitMQ server to process all messages in given queue
    #wait(workflow_event)
                        

if __name__ == '__main__':
    """ Parse command line arguments."""
    
    if len(sys.argv) < 3:
        raise Exception("Usage: python rabbitmq_producer.py <workflow_event> <number_of_events> [<metadata_key=metadata_value> <metadata_key=metadata_value> ...]")
    else:
        workflow_event = sys.argv[1]
        num_events = int( sys.argv[2] )
        # parse remaning arguments into a dictionary
        msg_dict = {}
        for arg in sys.argv[3:]:
            key, val = arg.split('=')
            msg_dict[key]=val

    publish_messages(workflow_event, num_events, msg_dict)
