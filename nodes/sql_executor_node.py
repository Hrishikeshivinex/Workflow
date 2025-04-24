from typing import Any, Dict
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from .base_node import BaseNode

logger = logging.getLogger("workflow.SQLExecutorNode")

class SQLExecutorNode(BaseNode):
    async def process(self, input_data: str) -> Dict[str, Any]:
        try:
            # Log connection attempt
            logger.info(f"Connecting to database at {self.data['db_host']}:{self.data['db_port']}/{self.data['db_name']}")
            
            # Create connection URL
            connection_string = (
                f"mysql+pymysql://{self.data['db_user']}:{self.data['db_password']}"
                f"@{self.data['db_host']}:{self.data['db_port']}/{self.data['db_name']}"
            )
            
            # Create engine
            engine = create_engine(connection_string, pool_recycle=3600)
            
            try:
                # Execute query
                with engine.connect() as connection:
                    logger.info(f"Successfully connected to database")
                    logger.info(f"Executing SQL query: {input_data}")
                    
                    df = pd.read_sql_query(text(input_data), connection)
                    logger.info(f"Query executed successfully, returned {len(df)} rows")
                    
                    # Format results
                    if 'total_sales' in df.columns:
                        df['total_sales'] = df['total_sales'].round(2)
                    if 'sales' in df.columns:
                        df['sales'] = df['sales'].round(2)
                    
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
            logger.error(f"Error in SQL execution: {str(e)}")
            return {
                "data": [],
                "metadata": {
                    "error": str(e),
                    "sql_query": input_data
                }
            } 