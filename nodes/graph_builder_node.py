from typing import Any, Dict
import json
import logging
from .base_node import BaseNode
from utils.logger import log_node_execution

logger = logging.getLogger("workflow.GraphBuilderNode")

class GraphBuilderNode(BaseNode):
    async def process(self, input_data: Dict) -> Dict:
        try:
            # Log input data structure
            logger.debug(f"Received input data structure: {json.dumps(input_data['metadata'], indent=2)}")
            
            # Extract data
            data = input_data.get("data", [])
            metadata = input_data.get("metadata", {})
            
            logger.debug(f"Processing {len(data)} records")
            
            # Log axes configuration
            logger.debug(f"Using x_axis: {self.data['x_axis']}, y_axis: {self.data['y_axis']}")
            
            # Prepare visualization config
            chart_config = {
                "type": self.data.get("graph_type", "bar"),
                "data": {
                    "labels": [item[self.data["x_axis"]] for item in data],
                    "datasets": [{
                        "label": "Sales",
                        "data": [item[self.data["y_axis"]] for item in data]
                    }]
                },
                "options": {
                    "title": {
                        "display": True,
                        "text": self.data.get("title", "Sales Data")
                    },
                    "scales": {
                        "x": {
                            "title": {
                                "display": True,
                                "text": self.data["x_axis"].replace("_", " ").title()
                            }
                        },
                        "y": {
                            "title": {
                                "display": True,
                                "text": self.data["y_axis"].replace("_", " ").title()
                            }
                        }
                    }
                }
            }
            
            # Log final configuration
            logger.debug(f"Generated chart configuration: {json.dumps(chart_config, indent=2)}")
            
            result = {
                "visualization": chart_config,
                "raw_data": data,
                "metadata": metadata
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error in graph building: {str(e)}")
            raise 