import logging
import json
from datetime import datetime
from typing import Any, Dict
import os
import sys

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Configure logging
def setup_logger():
    logger = logging.getLogger("workflow")
    logger.setLevel(logging.DEBUG)
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create handlers
    file_handler = logging.FileHandler('logs/workflow.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(file_formatter)
    
    # Remove any existing handlers
    logger.handlers = []
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def log_node_execution(node_type: str, node_id: str, input_data: Any, output_data: Any) -> None:
    """Log node execution details"""
    logger = logging.getLogger(f"workflow.{node_type}")
    
    try:
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "node_type": node_type,
            "node_id": node_id,
            "input": input_data,
            "output": output_data
        }
        logger.debug(json.dumps(log_entry, indent=2))
    except Exception as e:
        logger.error(f"Error logging node execution: {str(e)}") 