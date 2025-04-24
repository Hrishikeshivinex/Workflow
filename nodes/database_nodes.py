"""
Database-related nodes for the workflow system.
This file contains nodes for SQL execution, NLP-to-SQL conversion, and the node registry.
"""

from typing import Any, Dict
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from openai import AsyncOpenAI
from .base_node import BaseNode
from .input_node import InputNode
from .llm_node import LLMNode
from .graph_builder_node import GraphBuilderNode
from .time_trigger_node import TimeTriggerNode
from .prompt_node import PromptNode

# Set up loggers
sql_logger = logging.getLogger("workflow.SQLExecutorNode")
nlp_logger = logging.getLogger("workflow.NLPToSQLNode")

class SQLExecutorNode(BaseNode):
    """
    Executes SQL queries against a database and returns the results.
    
    Required data parameters:
    - db_host: Database host
    - db_port: Database port
    - db_user: Database username
    - db_password: Database password
    - db_name: Database name
    """
    
    async def process(self, input_data: str) -> Dict[str, Any]:
        try:
            # Log connection attempt
            sql_logger.info(f"Connecting to database at {self.data['db_host']}:{self.data['db_port']}/{self.data['db_name']}")
            
            # Create connection URL
            connection_string = (
                f"mysql+pymysql://{self.data['db_user']}:{self.data['db_password']}"
                f"@{self.data['db_host']}:{self.data['db_port']}/{self.data['db_name']}"
            )
            
            # Create engine and execute query
            engine = create_engine(connection_string, pool_recycle=3600)
            
            try:
                with engine.connect() as connection:
                    sql_logger.info(f"Successfully connected to database")
                    sql_logger.info(f"Executing SQL query: {input_data}")
                    
                    df = pd.read_sql_query(text(input_data), connection)
                    sql_logger.info(f"Query executed successfully, returned {len(df)} rows")
                    
                    # Format numeric results
                    for col in df.columns:
                        if df[col].dtype.kind in 'fc':  # float or complex
                            df[col] = df[col].round(2)
                    
                    # Prepare result
                    result = {
                        "data": df.to_dict('records'),
                        "metadata": {
                            "row_count": len(df),
                            "columns": list(df.columns),
                            "sql_query": input_data
                        }
                    }
                    
                    return result
            finally:
                engine.dispose()
                
        except Exception as e:
            sql_logger.error(f"Error in SQL execution: {str(e)}")
            return {
                "data": [],
                "metadata": {
                    "error": str(e),
                    "sql_query": input_data
                }
            }

class NLPToSQLNode(BaseNode):
    """
    Converts natural language questions to SQL queries using OpenAI.
    
    Required data parameters:
    - api_key: OpenAI API key
    """
    
    async def process(self, input_data: str) -> str:
        try:
            # Initialize OpenAI client
            client = AsyncOpenAI(api_key=self.data['api_key'])
            
            # Create the prompt with database schema information
            db_schema_info = """
            Convert the following natural language question to a SQL query.
            
            Database schema:
            Table: orders
            Columns:
            - region (text): North, South, East, West
            - sales (numeric): The sales amount
            - order_date (date): The date of the order
            - product_name (text): The name of the product
            - customer_name (text): The name of the customer
            
            Important guidelines:
            1. Always include product_name in your SELECT statement
            2. Always GROUP BY product_name when calculating aggregates
            3. Use SUM(sales) AS total_sales for sales aggregation
            4. Only return the SQL query without any explanations
            """
            
            prompt = f"{db_schema_info}\n\nQuestion: {input_data}\n\nSQL Query:"
            nlp_logger.info(f"Converting natural language to SQL: {input_data}")
            
            # Call OpenAI API
            response = await client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Extract and clean up the SQL
            result = response.choices[0].message.content.strip()
            
            # Remove SQL code block markers if present
            if result.startswith("```sql"):
                result = result.replace("```sql", "", 1)
            if result.endswith("```"):
                result = result[:-3]
            
            result = result.strip()
            nlp_logger.info(f"Generated SQL query: {result}")
            return result
            
        except Exception as e:
            nlp_logger.error(f"Error in NLP to SQL conversion: {str(e)}")
            raise

# Node registry mapping node types to their implementations
NODE_REGISTRY = {
    "Input": InputNode,
    "LLM": LLMNode,
    "SQLExecutor": SQLExecutorNode,
    "GraphBuilder": GraphBuilderNode,
    "TimeTrigger": TimeTriggerNode,
    "PromptNode": PromptNode,
    "NLPToSQL": NLPToSQLNode
} 