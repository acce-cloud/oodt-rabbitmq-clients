#!/usr/bin/env python

import sys
import os
import pika
import xmlrpclib
import time
import threading
import logging

logging.basicConfig(level=logging.INFO, format='(%(threadName)-10s) %(message)s')


class WorkflowManagerClient(object):
    '''
    Python client used to interact with a remote Workflow Manager via the XML/RPC API.
    Available methods are defined in Java class org.apache.oodt.cas.workflow.system.XmlRpcWorkflowManager.
    
    IMPORTANT: this class is NOT thread safe because xmlrpclib is NOT thread safe under Pythn 2.7
    '''
    
    def __init__(self, 
                 workflow_event,
                 workflowManagerUrl='http://localhost:9001/',
                 verbose=False):
        
        # connect to Workflow Manager server
        self.workflowManagerServerProxy = xmlrpclib.ServerProxy(workflowManagerUrl, verbose=verbose)
        
        # retrieve workflow definition
        logging.info('Workflow event: %s' % workflow_event)
        self.workflowTasks = self._getWorkflowTasks(workflow_event)
        logging.info('Workflow tasks: %s' % self.workflowTasks)
    
    def _getWorkflowTasks(self, workflow_event):
        '''Retrieves the workflow tasks by the triggering event.'''
        
        workflows =  self.workflowManagerServerProxy.workflowmgr.getWorkflowsByEvent(workflow_event)
        for workflow in workflows:
            tasks = []
            for task in workflow['tasks']:
                tasks.append(task['id'])
            return tasks # assume only one workflow for each event
        
        
    def executeWorkflow(self, metadata):
        '''
        Public method that submits a workflow using the specified metadata,
        then blocks until its completion.
        '''
        
        # submit workflow
        wInstId = self.workflowManagerServerProxy.workflowmgr.executeDynamicWorkflow(self.workflowTasks, metadata)

        # wait for workflow completion
        return self._waitForWorkflowCompletion(wInstId)
        
    
    def _waitForWorkflowCompletion(self, wInstId):
        ''' Monitors a workflow instance until it completes.'''
    
        # now use the workflow instance id to check for status, wait until completed
        running_status  = ['CREATED', 'QUEUED', 'STARTED', 'PAUSED']
        pge_task_status = ['STAGING INPUT', 'BUILDING CONFIG FILE', 'PGE EXEC', 'CRAWLING']
        finished_status = ['FINISHED', 'ERROR', 'METMISS']
        status = 'UNKNOWN'
        while (True):
            # wait for the server to instantiate this workflow before querying it
            # then wait in between status checks
            time.sleep(1)
            try:
                response = self.workflowManagerServerProxy.workflowmgr.getWorkflowInstanceById(wInstId)
                if response is not None:
                    status = response['status']
                    if status in running_status or status in pge_task_status:
                        logging.debug('Workflow instance=%s running with status=%s' % (wInstId, status))
                        
                    elif status in finished_status:
                        logging.info('Workflow instance=%s ended with status=%s' % (wInstId, status))
                        break
                    else:
                        logging.warn('UNRECOGNIZED WORKFLOW STATUS: %s' % status)
                        break
            # xmlrpclib will throw an Exception if the workflow instance is not instantiated
            except Exception as e:
                logging.warn(e.message)
            
        return status    
