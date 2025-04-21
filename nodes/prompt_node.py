from typing import Any, Dict
import logging
from .base_node import BaseNode
from utils.logger import log_node_execution

logger = logging.getLogger("workflow.PromptNode")

class PromptNode(BaseNode):
    async def process(self, input_data: Any) -> str:
        try:
            # Get the template from node data
            template = self.data.get("template", "")
            logger.debug(f"Using template: {template}")
            
            # If input_data is a dictionary, use it directly for formatting
            if isinstance(input_data, dict):
                formatted_prompt = template.format(**input_data)
            # If input_data is a string or other type, use it as 'input' variable
            else:
                formatted_prompt = template.format(input=input_data)
                
            logger.debug(f"Formatted prompt: {formatted_prompt}")
            
            return formatted_prompt
            
        except KeyError as e:
            logger.error(f"Missing key in template formatting: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error in prompt formatting: {str(e)}")
            raise 