from typing import Any, Dict
import asyncio
import logging
import datetime
from .base_node import BaseNode
from utils.logger import log_node_execution

logger = logging.getLogger("workflow.TimeTriggerNode")

class TimeTriggerNode(BaseNode):
    """
    A node that triggers workflow execution at specified time intervals.
    
    Configuration options in self.data:
    - interval_minutes: How often to trigger the workflow (in minutes)
    - start_time: When to start the first execution (ISO format, optional)
    - end_time: When to stop executing (ISO format, optional)
    - max_executions: Maximum number of times to execute (optional)
    """
    
    def __init__(self, node_id: str, data: Dict[str, Any], inputs: list, outputs: list):
        super().__init__(node_id, data, inputs, outputs)
        self.is_running = False
        self.execution_count = 0
        self.task = None
        
        # Set default values if not provided
        if 'interval_minutes' not in self.data:
            self.data['interval_minutes'] = 60  # Default: 1 hour
            
        logger.debug(f"TimeTriggerNode initialized with interval: {self.data['interval_minutes']} minutes")
    
    async def process(self, input_data: Any) -> Dict[str, Any]:
        """
        Process method returns trigger configuration information and passes through input_data.
        The actual triggering happens in start_trigger().
        """
        # Log the input data
        logger.debug(f"TimeTriggerNode received input_data: {input_data}")
        
        # IMPORTANT: For scheduled executions, we need to return the input data directly
        # This ensures it's passed correctly to the next node
        return input_data
    
    async def start_trigger(self, callback_function):
        """
        Start the time-based trigger that will call the callback function
        at the specified interval.
        
        Args:
            callback_function: Async function to call when trigger fires
        """
        if self.is_running:
            logger.warning(f"Trigger {self.id} is already running")
            return
            
        self.is_running = True
        self.execution_count = 0
        
        # Parse times if provided
        start_time = None
        end_time = None
        
        if self.data.get('start_time'):
            try:
                start_time = datetime.datetime.fromisoformat(self.data['start_time'])
            except ValueError:
                logger.error(f"Invalid start_time format: {self.data['start_time']}")
                
        if self.data.get('end_time'):
            try:
                end_time = datetime.datetime.fromisoformat(self.data['end_time'])
            except ValueError:
                logger.error(f"Invalid end_time format: {self.data['end_time']}")
        
        # Calculate initial delay if start_time is in the future
        initial_delay = 0
        if start_time and start_time > datetime.datetime.now():
            initial_delay = (start_time - datetime.datetime.now()).total_seconds()
            logger.info(f"Trigger {self.id} will start in {initial_delay} seconds")
        
        # Start the trigger loop as a background task
        self.task = asyncio.create_task(self._trigger_loop(
            callback_function, 
            self.data['interval_minutes'] * 60,  # Convert minutes to seconds
            initial_delay,
            end_time,
            self.data.get('max_executions')
        ))
        
        logger.info(f"Trigger {self.id} started with {self.data['interval_minutes']} minute interval")
    
    async def stop_trigger(self):
        """Stop the trigger loop"""
        if not self.is_running:
            return
            
        self.is_running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
            
        logger.info(f"Trigger {self.id} stopped after {self.execution_count} executions")
    
    async def _trigger_loop(self, callback, interval_seconds, initial_delay=0, 
                           end_time=None, max_executions=None):
        """Internal loop that handles the timing and execution"""
        try:
            # Wait for initial delay if needed
            if initial_delay > 0:
                await asyncio.sleep(initial_delay)
            
            while self.is_running:
                now = datetime.datetime.now()
                
                # Check if we've reached the end time
                if end_time and now >= end_time:
                    logger.info(f"Trigger {self.id} reached end time, stopping")
                    await self.stop_trigger()
                    break
                
                # Check if we've reached max executions
                if max_executions and self.execution_count >= max_executions:
                    logger.info(f"Trigger {self.id} reached max executions ({max_executions}), stopping")
                    await self.stop_trigger()
                    break
                
                # Execute the callback
                try:
                    logger.info(f"Trigger {self.id} firing (execution #{self.execution_count + 1})")
                    await callback()
                    self.execution_count += 1
                    log_node_execution(
                        self.__class__.__name__,
                        self.id,
                        None,
                        {"execution_count": self.execution_count}
                    )
                except Exception as e:
                    logger.error(f"Error in trigger callback: {str(e)}")
                
                # Wait for the next interval
                await asyncio.sleep(interval_seconds)
                
        except asyncio.CancelledError:
            logger.debug(f"Trigger {self.id} task cancelled")
            raise
        except Exception as e:
            logger.error(f"Error in trigger loop: {str(e)}")
            self.is_running = False 