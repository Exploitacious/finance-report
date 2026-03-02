import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from pathlib import Path
import aiofiles

# Load env vars
from dotenv import load_dotenv
load_dotenv()

from app.engine import generate_report_logic, REPORT_PATH

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Spawn background task
    task = asyncio.create_task(background_looper())
    yield
    # Shutdown: Cancel task if needed (optional, app shutdown kills it anyway usually)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

async def background_looper():
    # Initial delay to ensure server is up or just run immediately
    # We run immediately.
    while True:
        try:
            await generate_report_logic()
        except Exception as e:
            print(f"Critical error in background loop: {e}")
        
        # Sleep for 1 hour
        await asyncio.sleep(3600)

app = FastAPI(lifespan=lifespan)

@app.get("/report", response_class=PlainTextResponse)
async def get_report():
    if not REPORT_PATH.exists():
        return "Report is generating... please check back in a minute."
    
    async with aiofiles.open(REPORT_PATH, mode='r') as f:
        content = await f.read()
    return content

@app.get("/health")
def health_check():
    return {"status": "ok"}
