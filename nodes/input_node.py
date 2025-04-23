from typing import Any, Dict
from .base_node import BaseNode
import logging

logger = logging.getLogger(__name__)

class InputNode(BaseNode):
    async def process(self, input_data: Any) -> Any:
        key = self.data.get('key')
        
        # Handle dictionary input
        if isinstance(input_data, dict):
            if key in input_data:
                return input_data[key]
            else:
                # FALLBACK: If the key doesn't exist, use a default value
                logger.warning(f"Input key '{key}' not found in input data, using default value")
                return "Show me total sales by region"
        
        # Handle non-dictionary input (pass through)
        logger.warning(f"Input data is not a dictionary, passing through as-is")
        return input_data 