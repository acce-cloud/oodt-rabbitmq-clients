#!/usr/bin/env python

import sys
import os
import pika
import xmlrpclib
import time
import threading
import logging

STATE_RUNNING = "PGE EXEC"

# time interval in seconds before attempting to submit another workflow
# it allows the previous workflow to enter a 'RUNNING' state
TIME_INTERVAL = 1


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

        logging.info('WM Client started: connecting to WM server: %s, listening for events of type: %s, max number of concurrent workflow instances: %s' % (
            workflowManagerUrl, workflow_event, max_num_running_workflow_instances))
        self.workflow_event = workflow_event
        self.max_num_running_workflow_instances = max_num_running_workflow_instances

        # initialize the number of running workflow instances to 0
        self.num_running_workflow_instances = 0
        
        # flag to signify that the first workflow has been succesfully submitted,
        # so that the Workflow manager Lucene index has been properly initialized
        # no querying can take place before that
        self.init = False

    def isReady(self):
        '''
        Checks whether the number of workflow instances already running is already greater than the maximum allowed value.
        '''
                
        if self.init:

            # try executing XML/RPC query to update the number of running instances
            try:
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    self._get_all_workflows()
               
                response = self.workflowManagerServerProxy.workflowmgr.getNumWorkflowInstancesByStatus(STATE_RUNNING)
                self.num_running_workflow_instances = int(response)
                logging.info("Retrieved number of RUNNING workflow instances = %s" % self.num_running_workflow_instances)
                
            # error in XML/RPC communication
            except Exception as e:
                logging.warn("WM Client XML/RPC Error: %s" % e)             

        status = (self.num_running_workflow_instances < self.max_num_running_workflow_instances)
        logging.debug("WM Client ready status = %s" % status)
        return status
    
    def _get_all_workflows(self):
        '''Method to debug the status and number of all workflow instances.'''
        
        workflows = {}
        workflow_instances = self.workflowManagerServerProxy.workflowmgr.getWorkflowInstances()
        logging.debug("Retrieved number of TOTAL workflow instances = %s" % len(workflow_instances) )
        for workflow_instance in workflow_instances:
            wid = workflow_instance['id']
            status = workflow_instance['status']
            logging.debug("Workflow instance id=% status=%s" % (wid, status))
            if workflows.get(status, None):
                workflows[status].append(id)
            else:
                workflows[status] = [id]

        # print out summary
        for key, values in workflows.items():
            logging.debug("Workflows status=%s # = %s" % (key,len(values)))
            

    def submitWorkflow(self, metadata):
        '''
        Method that submits the workflow, then updates the number of running instances.
        '''

        try:

            # submit workflow
            logging.info('WM client: submitting workflow %s with metadata %s' % (self.workflow_event, metadata))
            self.workflowManagerServerProxy.workflowmgr.handleEvent(self.workflow_event, metadata)

            # workflow succesfully submitted
            logging.info("WM client: workflow %s with metadata %s succesfully submitted" % (self.workflow_event, metadata))
            self.num_running_workflow_instances += 1 # increment counter - prevents pulling too many messages from RMQ server
            self.init = True # can now query the Workflow Manager for running workflows
            time.sleep(TIME_INTERVAL)
            return True # success

        # error in XML/RPC communication
        except Exception as e:
            logging.warn("WM client: error submitting workflow %s with metadata %s" % (self.workflow_event, metadata))
            logging.warn("WM client: XML/RPC error: %s" % e)
            return False # error
