import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, RedirectResponse, HTMLResponse
from pathlib import Path
import aiofiles

# Load env vars
from dotenv import load_dotenv
load_dotenv()

from .config import DATA_DIR, REPORT_PATH, SCHWAB_TOKEN_FILE
from .engine import generate_report_logic

print(f"INFO: Initializing Finance Report API with DATA_DIR={DATA_DIR.absolute()}")
print(f"INFO: Primary Report Path: {REPORT_PATH.absolute()}")
print(f"INFO: Schwab Token Path: {SCHWAB_TOKEN_FILE.absolute()}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Startup: Clear news history on start to get a fresh batch for the first report
    news_state = DATA_DIR / ".news_state.json"
    if news_state.exists():
        print("Clearing news state for fresh pull...")
        try:
            news_state.unlink()
        except Exception as e:
            print(f"Error clearing news state: {e}")

    # Startup: Trigger first report generation IMMEDIATELY and wait for it
    # This ensures users don't have to wait an hour for the first report.
    print("INFO: Generating initial report before accepting requests...")
    try:
        await generate_report_logic()
    except Exception as e:
        print(f"CRITICAL ERROR during startup report generation: {e}")

    # Startup: Spawn background task for subsequent updates
    task = asyncio.create_task(background_looper())
    yield
    # Shutdown: Cancel task if needed
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

async def background_looper():
    # Since we already ran the first one in lifespan, we sleep first.
    while True:
        # Sleep for 1 hour
        await asyncio.sleep(3600)
        
        print(f"INFO: Triggering background report generation into {DATA_DIR.absolute()}")
        try:
            await generate_report_logic()
        except Exception as e:
            print(f"CRITICAL ERROR in background loop at {DATA_DIR.absolute()}: {e}")

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root_redirect():
    return RedirectResponse(url="/report")

@app.get("/report", response_class=PlainTextResponse)
@app.get("/current", response_class=PlainTextResponse)
async def get_report():
    print(f"INFO: Serving report request from {DATA_DIR.absolute()}")
    # Attempt to find the latest timestamped report first
    reports = sorted(list(DATA_DIR.glob("report-*.md")), reverse=True)
    if not reports:
        # Fallback to report.md if it exists
        if not REPORT_PATH.exists():
            print(f"WARN: Report file not found at {REPORT_PATH.absolute()}")
            return "Report is generating... please check back in a minute."
        target_path = REPORT_PATH
    else:
        target_path = reports[0]
    
    print(f"INFO: Reading report from {target_path.absolute()}")
    async with aiofiles.open(target_path, mode='r') as f:
        content = await f.read()
    return content

@app.get("/reports", response_class=HTMLResponse)
async def list_reports():
    if not DATA_DIR.exists():
        return "<h1>No reports directory found.</h1>"
    
    # List files matching report-*.md, sorted by newest first
    reports = sorted([f.name for f in DATA_DIR.glob("report-*.md")], reverse=True)
    
    if not reports:
        return "<h1>No reports found yet.</h1><p>The first report may still be generating.</p>"
    
    html = "<h1>Generated Finance Reports</h1><ul>"
    for r in reports:
        html += f'<li><a href="/reports/{r}">{r}</a></li>'
    html += "</ul>"
    html += '<br><a href="/report">View Latest Report</a>'
    return html

@app.get("/reports/{filename}", response_class=PlainTextResponse)
async def get_specific_report(filename: str):
    path = DATA_DIR / filename
    # Security check: ensure file is within DATA_DIR and has expected format
    if not path.exists() or not filename.startswith("report-") or not filename.endswith(".md"):
        return "Report not found."
    
    async with aiofiles.open(path, mode='r') as f:
        content = await f.read()
    return content

@app.get("/health")
def health_check():
    return {"status": "ok"}
