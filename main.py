from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Dict, Any, AsyncGenerator
import uuid
import asyncio
import os
import json
from datetime import datetime, timedelta
from flow.flow_config import FlowConfig
from utils.logger import setup_logger, log_node_execution

from langchain_community.chat_models import ChatOpenAI
from langchain_mistralai import ChatMistralAI
from langchain.schema import AIMessage, HumanMessage
from nodes.time_trigger_node import TimeTriggerNode

app = FastAPI()

# Setup logger
logger = setup_logger()

# ==== Node & Workflow Schemas ====
class Node(BaseModel):
    id: str
    type: str
    data: Dict[str, Any]
    inputs: List[str] = []
    outputs: List[str] = []

class Edge(BaseModel):
    source: str
    target: str

class Workflow(BaseModel):
    nodes: List[Node]
    edges: List[Edge]

class ExecuteRequest(BaseModel):
    workflow: Workflow
    inputs: Dict[str, Any]

class WorkflowRequest(BaseModel):
    inputs: Dict[str, Any]
    workflow: Dict[str, Any]

# ==== In-Memory Store (for example only) ====
saved_workflows: Dict[str, Workflow] = {}

# ==== Endpoint: Save Workflow ====
@app.post("/workflow", response_model=str)
def save_workflow(workflow: Workflow):
    workflow_id = str(uuid.uuid4())
    saved_workflows[workflow_id] = workflow
    return workflow_id

# ==== Endpoint: Load Workflow ====
@app.get("/workflow/{workflow_id}", response_model=Workflow)
def load_workflow(workflow_id: str):
    if workflow_id not in saved_workflows:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return saved_workflows[workflow_id]

# ==== Endpoint: Delete Workflow ====
@app.delete("/workflow/{workflow_id}", response_model=Dict[str, bool])
def delete_workflow(workflow_id: str):
    if workflow_id not in saved_workflows:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    # Remove the workflow from the dictionary
    del saved_workflows[workflow_id]
    logger.info(f"Workflow deleted: {workflow_id}")
    
    return {"success": True}

# ==== Helper: Streaming Generator ====
async def stream_llm_response(prompt: str, model: str, api_key: str) -> AsyncGenerator[str, None]:
    try:
        if model == "openai":
            llm = ChatOpenAI(
                api_key=api_key,
                streaming=True,
                temperature=0.7,
                model="gpt-3.5-turbo"
            )
        else:
            yield "[Unsupported model]"
            return

        async for chunk in llm.astream([HumanMessage(content=prompt)]):
            if chunk.content:
                yield chunk.content
                
    except Exception as e:
        yield f"[LLM Error: {str(e)}]"

# ==== Endpoint: Execute Workflow ====
@app.post("/execute/{workflow_id}")
async def execute_workflow_endpoint(workflow_id: str, request_data: WorkflowRequest):
    try:
        # Check if this workflow contains a TimeTriggerNode
        workflow = request_data.workflow
        trigger_nodes = [node for node in workflow["nodes"] if node["type"] == "TimeTrigger"]
        
        # If there are trigger nodes, set up the scheduled execution
        if trigger_nodes:
            # Check if this workflow is already scheduled
            if workflow_id in active_triggers:
                # Return information about the active trigger
                trigger_info = active_triggers[workflow_id]
                trigger_node = trigger_info["trigger_node"]
                
                # Include the input question and last result in the response
                input_key = None
                for node in workflow["nodes"]:
                    if node["type"] == "Input":
                        input_key = node["data"].get("key")
                        break
                
                input_question = request_data.inputs.get(input_key, "No input question found") if input_key else "No input key found"
                last_result = trigger_info.get("last_result", "No results yet")
                
                return {
                    "status": "already_scheduled",
                    "workflow_id": workflow_id,
                    "active_since": trigger_info["start_time"].isoformat(),
                    "execution_count": trigger_node.execution_count,
                    "interval_minutes": trigger_node.data.get("interval_minutes", 60),
                    "next_execution": (trigger_info["start_time"] + 
                                      timedelta(minutes=trigger_node.data.get("interval_minutes", 60) * 
                                               (trigger_node.execution_count + 1))).isoformat(),
                    "input_question": input_question,
                    "last_result": last_result
                }
            
            # Initialize the flow
            flow = FlowConfig(workflow)
            
            # For each trigger node, set up the trigger
            for node_config in trigger_nodes:
                node_id = node_config["id"]
                trigger_node = flow.nodes[node_id]
                
                # Define the callback function that will be called when the trigger fires
                async def trigger_callback():
                    logger.info(f"Executing scheduled workflow {workflow_id}")
                    try:
                        # Get the original request data from active_triggers
                        if "request_data" not in active_triggers[workflow_id]:
                            logger.error(f"No request_data found in active_triggers for workflow {workflow_id}")
                            logger.debug(f"active_triggers keys: {list(active_triggers[workflow_id].keys())}")
                            return
                        
                        original_request = active_triggers[workflow_id]["request_data"]
                        logger.debug(f"Original request inputs: {original_request.inputs}")
                        
                        # Find what key the Input node is looking for
                        input_key = None
                        for node in original_request.workflow["nodes"]:
                            if node["type"] == "Input":
                                input_key = node["data"].get("key")
                                break
                        
                        # If we found an input key, ensure it exists in the inputs
                        if input_key:
                            logger.debug(f"Input node is looking for key: {input_key}")
                            if not hasattr(original_request, 'inputs') or not original_request.inputs or input_key not in original_request.inputs:
                                logger.warning(f"Input key '{input_key}' not found in request_data, using default value")
                                if not hasattr(original_request, 'inputs') or original_request.inputs is None:
                                    original_request.inputs = {}
                                original_request.inputs[input_key] = "Show me total sales by region"
                        
                        # Execute the workflow with the original inputs and capture the result
                        result = await execute_workflow(workflow_id, original_request)
                        
                        # Store the result in the active_triggers dictionary
                        active_triggers[workflow_id]["last_result"] = result
                        
                        logger.info(f"Scheduled execution of workflow {workflow_id} completed")
                    except Exception as e:
                        logger.error(f"Error in scheduled execution of workflow {workflow_id}: {str(e)}")
                
                # Start the trigger
                await trigger_node.start_trigger(trigger_callback)
                
                # Store the active trigger
                active_triggers[workflow_id] = {
                    "trigger_node": trigger_node,
                    "start_time": datetime.now(),
                    "workflow": workflow,
                    "request_data": request_data
                }
            
            # Find the input question for the response
            input_key = None
            for node in workflow["nodes"]:
                if node["type"] == "Input":
                    input_key = node["data"].get("key")
                    break
            
            input_question = request_data.inputs.get(input_key, "No input question found") if input_key else "No input key found"
            
            # Return information about the scheduled workflow
            trigger_node = flow.nodes[trigger_nodes[0]["id"]]
            return {
                "status": "scheduled",
                "workflow_id": workflow_id,
                "trigger_count": len(trigger_nodes),
                "scheduled_at": datetime.now().isoformat(),
                "interval_minutes": trigger_node.data.get("interval_minutes", 60),
                "next_execution": (datetime.now() + 
                                  timedelta(minutes=trigger_node.data.get("interval_minutes", 60))).isoformat(),
                "input_question": input_question,
                "first_execution_pending": True
            }
        
        # If no trigger nodes, execute normally
        # Get the last node's ID (LLM node)
        last_node_id = workflow["nodes"][-1]["id"]
        
        # Initialize flow with the workflow configuration
        flow = FlowConfig(workflow)
        
        # Get LLM node configuration
        llm_node = next(node for node in workflow["nodes"] if node["id"] == last_node_id)
        
        # Get the prompt by executing up to the LLM node
        node_outputs = {}
        for node in workflow["nodes"]:
            if node["id"] == last_node_id:
                break
            if node["type"] == "Input":
                node_outputs[node["id"]] = request_data.inputs.get(node["data"].get("key"), None)
            elif node["type"] == "PromptNode":
                template = node["data"].get("template")
                input_value = node_outputs[node["inputs"][0]]
                if isinstance(input_value, dict):
                    node_outputs[node["id"]] = template.format(**input_value)
                else:
                    node_outputs[node["id"]] = template.format(input=input_value)
        
        # Get the final prompt
        prompt = node_outputs[llm_node["inputs"][0]]
        
        # Stream the LLM response
        return StreamingResponse(
            stream_llm_response(
                prompt=prompt,
                model=llm_node["data"]["model"],
                api_key=llm_node["data"]["api_key"]
            ),
            media_type="text/event-stream"
        )
        
    except Exception as e:
        logger.error(f"Error in workflow endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def execute_workflow(workflow_id: str, request_data: WorkflowRequest):
    start_time = datetime.now()
    
    try:
        logger.info(f"Starting workflow execution: {workflow_id}")
        
        # Find what key the Input node is looking for
        input_key = None
        for node in request_data.workflow["nodes"]:
            if node["type"] == "Input":
                input_key = node["data"].get("key")
                logger.debug(f"Found Input node with key: {input_key}")
                break
        
        # If we found an input key, ensure it exists in the inputs
        if input_key:
            logger.debug(f"Input node is looking for key: {input_key}")
            if not hasattr(request_data, 'inputs') or not request_data.inputs or input_key not in request_data.inputs:
                logger.warning(f"Input key '{input_key}' not found in request_data, using default value")
                if not hasattr(request_data, 'inputs') or request_data.inputs is None:
                    request_data.inputs = {}
                request_data.inputs[input_key] = "Show me total sales by region"
        
        # CRITICAL FIX: Log the exact input data being passed to the flow
        logger.debug(f"Input data before execution: {request_data.inputs}")
        
        # Initialize flow with the workflow configuration from the request
        flow = FlowConfig(request_data.workflow)
        
        # Execute flow with the input data
        results = await flow.execute(request_data.inputs)
        
        # Calculate execution time
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # Log final results
        logger.info(f"Workflow completed in {execution_time:.2f} seconds")
        
        return results
        
    except Exception as e:
        logger.error(f"Workflow execution failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def main():
    # Load flow configuration
    flow = FlowConfig('flow_config.json')
    
    # Execute flow with sample input
    input_data = {
        "question": "Show me total sales by region"
    }
    
    results = await flow.execute(input_data)
    
    # The final result will be a Plotly figure
    final_figure = results['4']  # Using the last node's ID
    final_figure.show()

# Add these to store active triggers
active_triggers = {}



# Add this endpoint to get all scheduled workflows
@app.get("/execute/scheduled")
def get_scheduled_workflows():
    """Get all scheduled workflows"""
    result = {}
    
    for workflow_id, trigger_info in active_triggers.items():
        trigger_node = trigger_info["trigger_node"]
        request_data = trigger_info["request_data"]
        
        # Find the input question
        input_key = None
        for node in request_data.workflow["nodes"]:
            if node["type"] == "Input":
                input_key = node["data"].get("key")
                break
        
        input_question = request_data.inputs.get(input_key, "No input question found") if input_key else "No input key found"
        last_result = trigger_info.get("last_result", "No results yet")
        
        result[workflow_id] = {
            "active_since": trigger_info["start_time"].isoformat(),
            "execution_count": trigger_node.execution_count,
            "interval_minutes": trigger_node.data.get("interval_minutes", 60),
            "next_execution": (trigger_info["start_time"] + 
                              timedelta(minutes=trigger_node.data.get("interval_minutes", 60) * 
                                       (trigger_node.execution_count + 1))).isoformat(),
            "input_question": input_question,
            "last_result": last_result
        }
    
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)