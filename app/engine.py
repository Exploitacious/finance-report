import json
import os
import sys
import asyncio
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import aiofiles

from app.sources.market import MarketData
from app.sources.sentiment import SentimentData
from app.sources.news import NewsData
from app.sources.schwab import SchwabMarketData
from app.sources.fred import FredSource
from app.config import TICKERS

DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
REPORT_PATH = DATA_DIR / "report.md"
TEMPLATE_PATH = Path(__file__).parent / "templates" / "report.md"

def load_template():
    with open(TEMPLATE_PATH, 'r') as f:
        return f.read()

def to_float(val, default=0.0):
    try:
        if val is None: return default
        return float(val)
    except (TypeError, ValueError):
        return default

def format_table(data_dict, tickers):
    table = "| Ticker | Price | RSI(14) | IVR | ZGL | GEX Sent | PCR |\n"
    table += "|---|---|---|---|---|---|---|\n"
    
    for t in tickers:
        info = data_dict.get(t, {})
        price = info.get('Price', 'N/A')
        if isinstance(price, (int, float)):
            price = f"{price:.2f}" if t in ['VIX', 'TNX', 'DXY'] else f"${price:.2f}"
            
        rsi = info.get('RSI_14', 'N/A')
        if isinstance(rsi, (int, float)): rsi = f"{rsi:.2f}"
        
        ivr = info.get('IVR', 'N/A')
        if isinstance(ivr, (int, float)): ivr = f"{ivr:.1f}"
        
        zgl = info.get('ZGL', '-')
        if isinstance(zgl, (int, float)): zgl = f"${zgl:.2f}"
        
        sent = info.get('GEX_Sentiment', '-')
        pcr = info.get('PCR_OI', '-')
        if isinstance(pcr, (int, float)): pcr = f"{pcr:.2f}"
        
        table += f"| **{t}** | {price} | {rsi} | {ivr} | {zgl} | {sent} | {pcr} |\n"
    return table

def format_report(template, data):
    market = data.get('market', {})
    # Schwab data is integrated into market source mostly, but we keep the structure just in case
    macro = market.get('macro', {})
    vol = market.get('volatility', {})
    rel = market.get('relative_strength', {})
    sentiment = data.get('sentiment', {})
    sent = sentiment.get('news_sentiment', {})
    cal = market.get('calendar', [])
    news = data.get('news', {}).get('fx_news', "No news available.")
    fred = data.get('fred', {})

    # Tables
    equity_table = format_table(market.get('equity_board', {}), ['SPY', 'QQQ', 'IWM'])
    macro_table = format_table(market.get('macro_board', {}), ['VIX', 'TNX', 'GLD', 'DXY', 'SLV'])

    # Earnings
    cal_rows = ""
    if isinstance(cal, list):
        for item in cal:
            if isinstance(item, dict):
                cal_rows += f"*   **{item.get('ticker', 'N/A')}:** {item.get('date', 'N/A')}\n"
    if not cal_rows:
        cal_rows = "*   No upcoming major earnings in watch list."

    # FRED
    todays_prints = fred.get('todays_prints', [])
    today_rows = ""
    if todays_prints:
        for p in todays_prints:
            today_rows += f"*   **{p['series_title']}:** {p['value']} (Period: {p['date']})\n"
    else:
        today_rows = "*   No key economic prints today."

    upcoming = fred.get('upcoming_calendar', [])
    upcoming_rows = ""
    if upcoming:
        for item in upcoming:
            upcoming_rows += f"*   **{item['release_name']}:** {item['date']}\n"
    else:
        upcoming_rows = "*   No major economic releases scheduled for next 30 days."

    # Prices for Macro section
    def get_price(ticker_key):
        # Check boards first
        for board in ['equity_board', 'macro_board']:
            p = market.get(board, {}).get(ticker_key, {}).get('Price')
            if p is not None:
                return p / 10.0 if ticker_key == 'TNX' else p
        return to_float(macro.get(ticker_key))

    context = {
        'date': datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S"),
        'sentiment_label': sent.get('Sentiment_Label', 'N/A'),
        'sentiment_score': sent.get('Average_Sentiment_Score', 'N/A'),
        'volatility_regime': vol.get('Term_Structure', 'N/A'),
        'credit_signal': macro.get('Credit_Signal', 'N/A'),
        'tnx': get_price('TNX'),
        'yield_curve_spread': to_float(macro.get('Yield_Curve_Spread')),
        'dxy': get_price('DXY'),
        'equity_board_table': equity_table,
        'macro_board_table': macro_table,
        'growth_value': to_float(rel.get('Growth_vs_Value_Ratio')),
        'risk_appetite': to_float(rel.get('Risk_Appetite_Ratio')),
        'curve_status': macro.get('Curve_Status', 'N/A'),
        'rotation_signal': rel.get('Rotation_Signal', 'N/A'),
        'calendar_rows': cal_rows,
        'today_prints': today_rows,
        'upcoming_calendar': upcoming_rows,
        'fx_news': news
    }

    return template.format(**context)

async def generate_report_logic():
    print(f"[{datetime.now(ZoneInfo('America/New_York'))}] Starting Finance Analyst Report Generation...")
    
    # Initialize sources
    # Schwab is optional/best-effort
    schwab = SchwabMarketData()
    
    sources = {
        'market': MarketData(schwab_source=schwab),
        'sentiment': SentimentData(),
        'news': NewsData(),
        'fred': FredSource()
    }

    aggregated_data = {}
    
    # Run fetches (could be parallelized with asyncio in the future if sources support async)
    # For now, we run synchronously inside this async wrapper, which blocks the loop briefly.
    # Ideally, we'd run_in_executor if these are blocking. 
    # Since this is a dedicated background task and 1h interval, blocking for a few seconds is fine.
    
    for name, source in sources.items():
        print(f"Fetching {name} data...")
        try:
            # Running synchronous fetch in thread pool to avoid blocking main loop completely
            loop = asyncio.get_running_loop()
            if name == 'schwab':
                # Schwab isn't used directly in aggregation key 'schwab' anymore in my refactor
                # but MarketData uses it.
                pass 
            else:
                aggregated_data[name] = await loop.run_in_executor(None, source.fetch)
        except Exception as e:
            print(f"Error fetching {name}: {e}")
            aggregated_data[name] = {}

    print("Generating report content...")
    try:
        template = load_template()
        report_content = format_report(template, aggregated_data)
    except Exception as e:
        print(f"Error formatting report: {e}")
        report_content = f"""# Error Generating Report

{e}"""

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Generate timestamped filename
    timestamp = datetime.now(ZoneInfo("America/New_York")).strftime("%Y%m%d-%H%M%S")
    timestamped_report_path = DATA_DIR / f"report-{timestamp}.md"

    # Write to both files
    print(f"Writing report to {REPORT_PATH} and {timestamped_report_path}...")
    try:
        async with aiofiles.open(REPORT_PATH, mode='w') as f:
            await f.write(report_content)
        async with aiofiles.open(timestamped_report_path, mode='w') as f:
            await f.write(report_content)
    except Exception as e:
        print(f"Error writing report files: {e}")
        
    print(f"[{datetime.now(ZoneInfo('America/New_York'))}] Report generation complete.")
