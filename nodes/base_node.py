from abc import ABC, abstractmethod
from typing import Any, Dict, List
import logging
from utils.logger import log_node_execution

logger = logging.getLogger("workflow.BaseNode")

class BaseNode(ABC):
    def __init__(self, node_id: str, data: Dict[str, Any], inputs: List[str], outputs: List[str]):
        self.id = node_id
        self.data = data
        self.input_nodes = inputs
        self.output_nodes = outputs
        
    async def execute_with_logging(self, input_data: Any) -> Any:
        """Execute node with logging"""
        try:
            # Log input
            log_node_execution(
                self.__class__.__name__,
                self.id,
                {"input_received": input_data},
                None
            )
            
            # Process
            result = await self.process(input_data)
            
            # Log output
            log_node_execution(
                self.__class__.__name__,
                self.id,
                None,
                {"output_generated": result}
            )
            
            return result
        except Exception as e:
            # Log error
            log_node_execution(
                self.__class__.__name__,
                self.id,
                None,
                {"error": str(e)}
            )
            raise
    
    @abstractmethod
    async def process(self, input_data: Any) -> Any:
        """Process the input data and return the result"""
        pass 