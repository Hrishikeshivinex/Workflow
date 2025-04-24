from typing import Any, Dict
import json
import logging
from .base_node import BaseNode
from utils.logger import log_node_execution

logger = logging.getLogger("workflow.GraphBuilderNode")

class GraphBuilderNode(BaseNode):
    async def process(self, input_data: Any) -> Dict[str, Any]:
        try:
            # Check if we have valid input data
            if not input_data or not isinstance(input_data, dict) or "data" not in input_data:
                logger.error(f"Invalid input data format: {input_data}")
                return {
                    "error": "Invalid input data format",
                    "visualization": None,
                    "raw_data": input_data
                }
            
            # Extract data from input
            data = input_data.get("data", [])
            metadata = input_data.get("metadata", {})
            
            # Check if there's an error in the input data
            if "error" in metadata:
                logger.error(f"Error in input data: {metadata['error']}")
                return {
                    "error": metadata["error"],
                    "visualization": None,
                    "raw_data": data,
                    "metadata": metadata
                }
            
            # Check if data is empty
            if not data:
                logger.warning("No data to visualize")
                return {
                    "error": "No data to visualize",
                    "visualization": None,
                    "raw_data": [],
                    "metadata": metadata
                }
            
            # Get configuration
            x_axis = self.data.get("x_axis", "product_name")
            y_axis = self.data.get("y_axis", "total_sales")
            graph_type = self.data.get("graph_type", "bar")
            title = self.data.get("title", "Data Visualization")
            
            # Check if required columns exist in the data
            first_row = data[0] if data else {}
            
            # If x_axis column is missing, try to handle it
            if x_axis not in first_row:
                logger.warning(f"Column '{x_axis}' not found in data. Available columns: {list(first_row.keys())}")
                
                # If there's only one row and no product_name, create a single bar chart
                if len(data) == 1 and "total_sales" in first_row:
                    # Create a single bar chart with a generic label
                    logger.info("Creating a single bar chart with the total sales value")
                    
                    # Use a generic label for the x-axis
                    labels = ["Total"]
                    values = [data[0]["total_sales"]]
                    
                    # Create a simple bar chart
                    visualization = {
                        "type": "bar",
                        "data": {
                            "labels": labels,
                            "datasets": [
                                {
                                    "label": "Sales",
                                    "data": values
                                }
                            ]
                        },
                        "options": {
                            "title": {
                                "display": True,
                                "text": title
                            },
                            "scales": {
                                "x": {
                                    "title": {
                                        "display": True,
                                        "text": "Region"
                                    }
                                },
                                "y": {
                                    "title": {
                                        "display": True,
                                        "text": "Total Sales"
                                    }
                                }
                            }
                        }
                    }
                    
                    return {
                        "visualization": visualization,
                        "raw_data": data,
                        "metadata": metadata
                    }
                
                # If there are other columns we could use as x_axis
                available_columns = list(first_row.keys())
                if available_columns:
                    # Try to find a suitable column for x_axis
                    for column in ["product_name", "region", "customer_name", "order_date"]:
                        if column in available_columns:
                            logger.info(f"Using '{column}' as x_axis instead of '{x_axis}'")
                            x_axis = column
                            break
                    else:
                        # If no suitable column found, use the first column
                        x_axis = available_columns[0]
                        logger.info(f"Using '{x_axis}' as x_axis")
            
            # If y_axis column is missing, try to handle it
            if y_axis not in first_row:
                logger.warning(f"Column '{y_axis}' not found in data. Available columns: {list(first_row.keys())}")
                
                # Try to find a suitable column for y_axis
                available_columns = list(first_row.keys())
                if available_columns:
                    for column in ["total_sales", "sales", "amount", "value"]:
                        if column in available_columns:
                            logger.info(f"Using '{column}' as y_axis instead of '{y_axis}'")
                            y_axis = column
                            break
                    else:
                        # If no suitable column found, use the first numeric column
                        for column in available_columns:
                            if isinstance(first_row[column], (int, float)):
                                y_axis = column
                                logger.info(f"Using '{y_axis}' as y_axis")
                                break
                        else:
                            # If no numeric column found, return an error
                            return {
                                "error": f"No numeric column found for y_axis",
                                "visualization": None,
                                "raw_data": data,
                                "metadata": metadata
                            }
            
            # Extract labels and values
            labels = [item[x_axis] for item in data]
            values = [item[y_axis] for item in data]
            
            # Create visualization based on graph type
            visualization = {
                "type": graph_type,
                "data": {
                    "labels": labels,
                    "datasets": [
                        {
                            "label": "Sales",
                            "data": values
                        }
                    ]
                },
                "options": {
                    "title": {
                        "display": True,
                        "text": title
                    },
                    "scales": {
                        "x": {
                            "title": {
                                "display": True,
                                "text": x_axis.replace("_", " ").title()
                            }
                        },
                        "y": {
                            "title": {
                                "display": True,
                                "text": y_axis.replace("_", " ").title()
                            }
                        }
                    }
                }
            }
            
            return {
                "visualization": visualization,
                "raw_data": data,
                "metadata": metadata
            }
            
        except Exception as e:
            logger.error(f"Error in graph building: {str(e)}")
            raise 