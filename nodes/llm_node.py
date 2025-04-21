from typing import Any, Dict
from openai import AsyncOpenAI
import logging
from .base_node import BaseNode
from utils.logger import log_node_execution

logger = logging.getLogger("workflow.LLMNode")

class LLMNode(BaseNode):
    async def process(self, input_data: Any) -> str:
        try:
            # Initialize AsyncOpenAI client
            client = AsyncOpenAI(api_key=self.data['api_key'])
            
            # Log API key (last 4 characters only)
            api_key = self.data['api_key']
            masked_key = f"...{api_key[-4:]}" if api_key else "None"
            logger.debug(f"Using API key ending in: {masked_key}")
            
            # Use the input directly as the prompt
            prompt = input_data
            logger.debug(f"Using prompt: {prompt}")
            
            # Call OpenAI API with async client
            logger.debug("Calling OpenAI API...")
            response = await client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Extract and log response
            result = response.choices[0].message.content.strip()
            logger.debug(f"Generated response: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in LLM processing: {str(e)}")
            raise 