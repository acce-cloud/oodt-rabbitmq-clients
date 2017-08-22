'''
Server proxy for the OODT Workflow Manager.
This class intercepts XML/RPC requests sent by clients to the Workflow Manager,
and sends them to a RabbitMQ server instead, 
for later consumption by RabbitMQ/OODT clients.
'''

from SimpleXMLRPCServer import SimpleXMLRPCServer
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)

# main server class
class WorkflowManagerProxy():

    def __init__(self):
        pass

    def sendEvent(self, event_name, metadata):
        logging.info("WorkflowManagerProxy.sendEvent(): event_name=%s metadata=%s" % (event_name, metadata) )

        return True



if __name__ == "__main__":

    server = SimpleXMLRPCServer(('localhost', 9000), logRequests=True, allow_none=True)
    server.register_instance(WorkflowManagerProxy())

    try:
        print 'Use Control-C to exit'
        server.serve_forever()
    except KeyboardInterrupt:
        print 'Exiting'
