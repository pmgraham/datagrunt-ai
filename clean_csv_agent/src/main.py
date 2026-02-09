import os

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Import the agent from our local module
# Adjust import path based on structure
try:
    from .agent import data_scientist_agent
except ImportError:
    from agent import data_scientist_agent

# Initialize FastAPI
app = FastAPI(title="DataGrunt Scientist API")
load_dotenv()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allow all origins for dev/testing.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AuditRequest(BaseModel):
    file_path: str

@app.post("/audit")
async def audit_dataset(file_path: str):
    """
    Run the Data Scientist Agent on a CSV file.
    """
    if not os.path.exists(file_path):
        raise HTTPException(status_code=400, detail=f"File not found: {file_path}")

    # Invoke the agent
    # Note: google-adk Agent 'query' or 'invoke' method might vary.
    # Standard pattern: response = agent.query(input_text)
    # Using the prompt defined in agent.py implicitly via instruction, 
    # we just need to pass the file path context or request.
    
    prompt = f"Please audit this file: {file_path}"
    
    try:
        # Assuming synchronous for now, or await if async supported
        response = data_scientist_agent.query(prompt)
        return {"result": str(response)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
