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
            
            # Prepare prompt
            prompt = f"""
            Generate a SQL query for: {input_data}
            
            Table structure:
            CREATE TABLE orders (
                order_id INT PRIMARY KEY AUTO_INCREMENT,
                region VARCHAR(50),
                sales DECIMAL(10,2),
                order_date DATE,
                product_name VARCHAR(100),
                customer_name VARCHAR(100)
            );
            
            Requirements:
            1. Return data in a format suitable for visualization
            2. For region-specific queries, use WHERE LOWER(region) = LOWER('<region>')
            3. Include relevant grouping and aggregations
            4. Return only the SQL query, no explanations
            """
            
            logger.debug(f"Prepared prompt: {prompt}")
            
            # Call OpenAI API with async client
            logger.debug("Calling OpenAI API...")
            response = await client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Extract and log SQL query
            sql_query = response.choices[0].message.content.strip()
            logger.debug(f"Generated SQL query: {sql_query}")
            
            return sql_query
            
        except Exception as e:
            logger.error(f"Error in LLM processing: {str(e)}")
            raise 