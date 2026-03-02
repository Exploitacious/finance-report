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

from app.engine import generate_report_logic, REPORT_PATH, DATA_DIR

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Clear news history on start to get a fresh batch for the first report
    news_state = DATA_DIR / ".news_state.json"
    if news_state.exists():
        print("Clearing news state for fresh pull...")
        try:
            news_state.unlink()
        except Exception as e:
            print(f"Error clearing news state: {e}")

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
@app.get("/current", response_class=PlainTextResponse)
async def get_report():
    if not REPORT_PATH.exists():
        return "Report is generating... please check back in a minute."
    
    async with aiofiles.open(REPORT_PATH, mode='r') as f:
        content = await f.read()
    return content

@app.get("/reports")
async def list_reports():
    if not DATA_DIR.exists():
        return {"reports": []}
    
    # List files matching report-*.md, sorted by newest first
    reports = sorted([f.name for f in DATA_DIR.glob("report-*.md")], reverse=True)
    return {"reports": reports}

@app.get("/reports/{filename}", response_class=PlainTextResponse)
async def get_specific_report(filename: str):
    path = DATA_DIR / filename
    if not path.exists() or not filename.startswith("report-") or not filename.endswith(".md"):
        return "Report not found."
    
    async with aiofiles.open(path, mode='r') as f:
        content = await f.read()
    return content

@app.get("/health")
def health_check():
    return {"status": "ok"}
