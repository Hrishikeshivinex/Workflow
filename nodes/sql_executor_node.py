from typing import Any, Dict
from sqlalchemy import create_engine, text
import pandas as pd
import logging
from .base_node import BaseNode
from utils.logger import log_node_execution

logger = logging.getLogger("workflow.SQLExecutorNode")

class SQLExecutorNode(BaseNode):
    async def process(self, input_data: str) -> Any:
        try:
            # Log connection parameters (without sensitive info)
            logger.debug(f"Connecting to database at {self.data['db_host']}:{self.data['db_port']}")
            
            # Create connection URL with proper escaping
            connection_string = (
                f"mysql+pymysql://{self.data['db_user']}:{self.data['db_password']}"
                f"@{self.data['db_host']}:{self.data['db_port']}/{self.data['db_name']}"
            )
            
            # Create engine with extended configuration
            engine = create_engine(
                connection_string,
                pool_recycle=3600,
                connect_args={
                    'ssl': self.data.get('ssl', {}),  # Optional SSL config
                    'connect_timeout': 10,  # Connection timeout in seconds
                }
            )
            
            try:
                # Test connection before executing query
                with engine.connect() as connection:
                    logger.debug("Database connection established successfully")
                    
                    # Execute query
                    logger.debug(f"Executing SQL query: {input_data}")
                    df = pd.read_sql_query(text(input_data), connection)
                    logger.debug(f"Query returned {len(df)} rows")
                    
                    # Format results
                    if 'total_sales' in df.columns:
                        df['total_sales'] = df['total_sales'].round(2)
                    if 'sales' in df.columns:
                        df['sales'] = df['sales'].round(2)
                    
                    result = {
                        "data": df.to_dict('records'),
                        "metadata": {
                            "row_count": len(df),
                            "columns": list(df.columns)
                        }
                    }
                    
                    logger.debug("Query execution completed successfully")
                    return result
                    
            finally:
                engine.dispose()
                logger.debug("Database connection closed")
                
        except Exception as e:
            logger.error(f"Error in SQL execution: {str(e)}")
            # Add more detailed error information
            error_details = {
                "error": str(e),
                "type": type(e).__name__,
                "connection_info": {
                    "host": self.data['db_host'],
                    "port": self.data['db_port'],
                    "user": self.data['db_user'],
                    "database": self.data['db_name']
                }
            }
            logger.error(f"Detailed error information: {error_details}")
            raise 