from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Dict, Any, AsyncGenerator
import uuid
import asyncio
import os
import json
from datetime import datetime
from flow.flow_config import FlowConfig
from utils.logger import setup_logger, log_node_execution

from langchain.chat_models import ChatOpenAI
from langchain_mistralai import ChatMistralAI
from langchain.schema import AIMessage, HumanMessage

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

# ==== Helper: Streaming Generator ====
async def stream_llm_response(prompt: str, model: str, api_key: str) -> AsyncGenerator[str, None]:
    try:
        if model == "openai":
            llm = ChatOpenAI(api_key=api_key, streaming=True, temperature=0.7)
        elif model == "mistral":
            llm = ChatMistralAI(api_key=api_key, streaming=True, temperature=0.7)
        else:
            yield "[Unsupported model]"
            return

        async for chunk in llm.astream([HumanMessage(content=prompt)]):
            yield chunk.content
    except Exception as e:
        yield f"[LLM Error: {str(e)}]"

# ==== Endpoint: Execute Workflow ====
@app.post("/execute/{workflow_id}")
async def execute_workflow_endpoint(workflow_id: str, request_data: WorkflowRequest):
    try:
        result = await execute_workflow(workflow_id, request_data)
        return {"status": "success", "data": result}
    except Exception as e:
        logger.error(f"Error in workflow endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def execute_workflow(workflow_id: str, request_data: WorkflowRequest):
    start_time = datetime.now()
    
    try:
        logger.info(f"Starting workflow execution: {workflow_id}")
        logger.debug(f"Input data: {json.dumps(request_data.inputs, indent=2)}")
        logger.debug(f"Workflow config: {json.dumps(request_data.workflow, indent=2)}")
        
        # Initialize flow with the workflow configuration from the request
        flow = FlowConfig(request_data.workflow)
        
        # Execute flow
        results = await flow.execute(request_data.inputs)
        
        # Calculate execution time
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # Log final results
        logger.info(f"Workflow completed in {execution_time:.2f} seconds")
        logger.debug(f"Final results: {json.dumps(results, indent=2)}")
        
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)