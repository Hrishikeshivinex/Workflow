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
            
            # Use the input directly as the prompt or format it if a prompt template is provided
            if 'prompt' in self.data and isinstance(input_data, str):
                # Format the prompt template with the input
                prompt = self.data['prompt'].format(question=input_data)
                
                # Add database schema information to the prompt
                db_schema_info = """
                Important: Our database has a table called 'orders' with these columns:
                - region (text): North, South, East, West
                - sales (numeric): The sales amount
                - order_date (date): The date of the order
                - product_name (text): The name of the product
                - customer_name (text): The name of the customer
                
                Do NOT use any tables named 'sales' - use the 'orders' table instead.
                
                IMPORTANT: Always include product_name in your SELECT statement and GROUP BY product_name
                to ensure the graph can be built correctly. The GraphBuilder node requires data to be 
                grouped by product_name with a total_sales column.
                """
                
                prompt = f"{prompt}\n\n{db_schema_info}"
                logger.debug(f"Formatted prompt from template with schema info: {prompt}")
            else:
                # Use the input directly
                prompt = input_data
                logger.debug(f"Using direct prompt: {prompt}")
            
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
            
            # If output_format is specified, try to format the result
            if 'output_format' in self.data and self.data['output_format'] == 'sql':
                # Try to extract SQL from the response if it's not already just SQL
                if "```sql" in result:
                    # Extract SQL from code blocks
                    import re
                    sql_match = re.search(r"```sql\s*(.*?)\s*```", result, re.DOTALL)
                    if sql_match:
                        result = sql_match.group(1).strip()
                        logger.debug(f"Extracted SQL from response: {result}")
                
                # Remove any explanations before or after the SQL
                if result.lower().startswith("select"):
                    # It's already a clean SQL query
                    pass
                elif "select" in result.lower():
                    # Try to extract just the SQL part
                    lines = result.split('\n')
                    sql_lines = []
                    for line in lines:
                        if any(keyword in line.lower() for keyword in ["select", "from", "where", "group by", "order by"]):
                            sql_lines.append(line)
                    if sql_lines:
                        result = '\n'.join(sql_lines)
                        logger.debug(f"Cleaned SQL query: {result}")
            
                # Ensure the query uses the correct table name
                if "from sales" in result.lower():
                    result = result.lower().replace("from sales", "FROM orders")
                    logger.debug(f"Fixed table name in query: {result}")
                
                # Ensure the query uses the correct column names
                if "sales_amount" in result.lower():
                    result = result.lower().replace("sales_amount", "sales")
                    logger.debug(f"Fixed column name in query: {result}")
            
                # Ensure the query includes product_name and groups by it
                if "product_name" not in result.lower():
                    # This is a simple query without product_name, let's fix it
                    if "where region = 'north'" in result.lower() or "where region='north'" in result.lower():
                        # Replace the entire query with a properly formatted one
                        result = """
                        SELECT product_name, SUM(sales) AS total_sales
                        FROM orders
                        WHERE region = 'North'
                        GROUP BY product_name
                        ORDER BY total_sales DESC
                        """
                        logger.debug(f"Replaced query to include product_name: {result}")
                    elif "group by" not in result.lower():
                        # Add product_name to the SELECT and add a GROUP BY
                        if "select sum" in result.lower():
                            result = result.lower().replace("select sum", "SELECT product_name, SUM")
                            result += "\nGROUP BY product_name"
                            logger.debug(f"Added product_name to query: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in LLM processing: {str(e)}")
            raise 