from typing import Dict, List, Any
import json
from nodes.input_node import InputNode
from nodes.llm_node import LLMNode
from nodes.sql_executor_node import SQLExecutorNode
from nodes.graph_builder_node import GraphBuilderNode
import logging

logger = logging.getLogger("workflow.FlowConfig")

class FlowConfig:
    NODE_TYPES = {
        'Input': InputNode,
        'LLM': LLMNode,
        'SQLExecutor': SQLExecutorNode,
        'GraphBuilder': GraphBuilderNode
    }
    
    def __init__(self, workflow_config: Dict):
        """Initialize with workflow configuration dictionary"""
        self.config = workflow_config
        self.nodes = {}
        self.build_nodes()
    
    def build_nodes(self):
        try:
            for node_config in self.config['nodes']:
                node_type = node_config['type']
                if node_type not in self.NODE_TYPES:
                    raise ValueError(f"Unknown node type: {node_type}")
                
                node_class = self.NODE_TYPES[node_type]
                
                self.nodes[node_config['id']] = node_class(
                    node_id=node_config['id'],
                    data=node_config['data'],
                    inputs=node_config['inputs'],
                    outputs=node_config['outputs']
                )
                logger.debug(f"Created node: {node_type} with ID: {node_config['id']}")
        except Exception as e:
            logger.error(f"Error building nodes: {str(e)}")
            raise
    
    async def execute(self, input_data: Dict[str, Any]):
        results = {}
        
        try:
            # Execute nodes in topological order
            for node_id, node in self.nodes.items():
                logger.debug(f"Executing node: {node_id}")
                
                if not node.input_nodes:
                    # Input node
                    results[node_id] = await node.execute_with_logging(input_data)
                else:
                    # Get input from previous nodes
                    node_input = results[node.input_nodes[0]]  # Assuming single input for simplicity
                    results[node_id] = await node.execute_with_logging(node_input)
                
                logger.debug(f"Node {node_id} execution completed")
            
            return results
            
        except Exception as e:
            logger.error(f"Error executing workflow: {str(e)}")
            raise 