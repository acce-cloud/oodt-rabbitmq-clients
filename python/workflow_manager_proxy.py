'''
Server proxy for the OODT Workflow Manager.
This class intercepts XML/RPC requests sent by clients to the Workflow Manager,
and sends them to a RabbitMQ server instead, 
for later consumption by RabbitMQ/OODT clients.
'''

from SimpleXMLRPCServer import SimpleXMLRPCServer
from rabbitmq_producer_daemon import RabbitmqProducerDaemon
import logging
import os
from urlparse import urlparse


# Set up logging
logging.basicConfig(level=logging.DEBUG)

# class used to enable "workflowmgr.handleEvent" notation
class ServiceRoot:
    pass

# main server class
class WorkflowManagerProxy():

    def __init__(self, rabbitmq_url):
        '''Starts the RabbitMQ producer daemon.'''
        
        # start RabbitMQ producer daemon
        self._rmqpd = RabbitmqProducerDaemon(rabbitmq_url)
        self._rmqpd.start()


    def handleEvent(self, event_name, metadata):
        '''Sends a message to start a workflow.'''

        logging.info("WorkflowManagerProxy.handleEvent(): event_name=%s metadata=%s" % (event_name, metadata) )
        
        status = self._rmqpd.publish_message(event_name, metadata)

        return status
    
    def _stop(self):
        '''Stops the RabbitMQ producer daemon.'''
        
        self._rmqpd.stop()
        

if __name__ == "__main__":
    
    # listen on host, port specified by $WORKFLOW_URL
    workflow_url = urlparse( os.getenv("WORKFLOW_URL","http://localhost:9001") )
    logging.info("Starting WorkflowManagerProxy for hostname=%s port=%s" % (workflow_url.hostname, workflow_url.port))
    server = SimpleXMLRPCServer( (workflow_url.hostname, int(workflow_url.port)), logRequests=True, allow_none=True)
    
    # send message to RabbitMQ server specified by $RABBITMQ_USER_URL
    rabbitmq_url = os.getenv("RABBITMQ_USER_URL","amqp://guest:guest@localhost/%2f")
    

    root = ServiceRoot()
    root.workflowmgr = WorkflowManagerProxy(rabbitmq_url)
    server.register_instance(root, allow_dotted_names=True)

    try:
        logging.info('Use Control-C to stop the  WorkflowManagerProxy')
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info('Stopping WorkflowManagerProxy...')
