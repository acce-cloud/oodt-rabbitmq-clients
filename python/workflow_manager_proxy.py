'''
Server proxy for the OODT Workflow Manager.
This class intercepts XML/RPC requests sent by clients to the Workflow Manager,
and sends them to a RabbitMQ server instead, 
for later consumption by RabbitMQ/OODT clients.
'''

from SimpleXMLRPCServer import SimpleXMLRPCServer
from rabbitmq_producer import publish_messages
import logging


# Set up logging
logging.basicConfig(level=logging.DEBUG)

# class used to enable "workflowmgr.handleEvent" notation
class ServiceRoot:
    pass

# main server class
class WorkflowManagerProxy():

    def __init__(self):
        pass

    def handleEvent(self, event_name, metadata):

        logging.info("WorkflowManagerProxy.handleEvent(): event_name=%s metadata=%s" % (event_name, metadata) )

        # FIXME
        publish_messages(event_name, 1, metadata)

        return True



if __name__ == "__main__":

    server = SimpleXMLRPCServer(('localhost', 9001), logRequests=True, allow_none=True)

    root = ServiceRoot()
    root.workflowmgr = WorkflowManagerProxy()
    server.register_instance(root, allow_dotted_names=True)

    try:
        print 'Use Control-C to exit'
        server.serve_forever()
    except KeyboardInterrupt:
        print 'Exiting'
