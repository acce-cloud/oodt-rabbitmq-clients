#!/usr/bin/env python

import sys
import os
import pika
import xmlrpclib
import time
import threading
import logging

logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)-10s) %(message)s')

STATE_RUNNING = "PGETask_Running"

class WorkflowManagerClient(object):
    '''
    Python client used to interact with a remote Workflow Manager via the XML/RPC API.
    Available methods are defined in Java class org.apache.oodt.cas.workflow.system.XmlRpcWorkflowManager.

    IMPORTANT: this class is NOT thread safe because xmlrpclib is NOT thread safe under Pythn 2.7

    IMPORTANT: the workflow manager cannot be queried before the first worklow is submitted,
    because the Lucene index is not initialized. Therefore, this client must first submit a job,
    then start querying the workflow manager for the number of workflow instances that are running.
    '''

    def __init__(self,
                 workflow_event,
                 workflowManagerUrl='http://localhost:9001/',
                 verbose=False,
                 max_num_running_workflow_instances=1):

        # connect to Workflow Manager server
        self.workflowManagerServerProxy = xmlrpclib.ServerProxy(
            workflowManagerUrl, verbose=verbose)

        logging.info('Workflow event: %s max number of concurrent workflow instances: %s' % (
            workflow_event, max_num_running_workflow_instances))
        self.workflow_event = workflow_event
        self.max_num_running_workflow_instances = max_num_running_workflow_instances

        # initialize the number of running workflow instances to 0
        self.num_running_workflow_instances = 0
        
        # flag to first submit a workflow before executing any querying
        # (since the first workflow initializes the Lucene index)
        self.first = True

    def isReady(self):
        '''
        Checks whether the number of workflow instances already running is already greater than the maximum allowed value.
        '''
                
        # always submit the first workflow immediately
        if self.first:
            self.first = False
            return True
        
        # all other workflow requests
        else:

            # try executing XML/RPC query to update the number of running instances
            try:
                response = self.workflowManagerServerProxy.workflowmgr.getNumWorkflowInstancesByStatus(STATE_RUNNING)
                self.num_running_workflow_instances = int(response)
                logging.info("Retrieved number of running workflows = %s" % self.num_running_workflow_instances)
                
                if self.num_running_workflow_instances < self.max_num_running_workflow_instances:
                    return True
                
            # error in XML/RPC communication
            except Exception as e:
                #logging.warn("XML/RPC Error: %s" % e.message)
                pass

            # not ready
            return False

    def submitWorkflow(self, metadata):
        '''
        Method that submits the workflow, then updates the number of running instances.
        '''

        try:

            # submit workflow
            logging.info('WM client: submitting workflow %s with metadata %s' % (self.workflow_event, metadata))
            self.workflowManagerServerProxy.workflowmgr.handleEvent(self.workflow_event, metadata)

            # workflow succesfully submitted
            logging.warn("WM client: workflow %s with metadata %s succesfully submitted" % (self.workflow_event, metadata))
            return True # success

        # error in XML/RPC communication
        except Exception as e:
            logging.warn("WM client: error submitting workflow %s with metadata %s" % (self.workflow_event, metadata))
            logging.warn("WM client: XML/RPC error: %s" % e.message)
            return False # error
