import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add the project root to sys.path
sys.path.append(str(Path(__file__).parent))

# Load environment variables
load_dotenv()

# Mocking the /app/data path for local testing
os.environ["DATA_DIR"] = "/tmp/finance-report-data"
Path("/tmp/finance-report-data").mkdir(parents=True, exist_ok=True)

# For schwab.py, we need to handle its hardcoded /app/tokens/tokens.json
# or ensure it can find the one in /root/finance-report-tokens/tokens.json
# Let's fix schwab.py first to use an ENV var.

from app.sources.market import MarketData
from app.sources.sentiment import SentimentData
from app.sources.news import NewsData
from app.sources.fred import FredSource
from app.sources.schwab import SchwabMarketData

def test_news():
    print("Testing News (FXStreet RSS)...")
    try:
        news = NewsData()
        res = news.fetch()
        print(f"SUCCESS: Found {res.get('fx_news', '').count('URL')} articles.")
    except Exception as e:
        print(f"FAILED: News fetch failed: {e}")

def test_fred():
    print("Testing FRED...")
    try:
        fred = FredSource()
        res = fred.fetch()
        print(f"SUCCESS: Found {len(res.get('todays_prints', []))} today prints and {len(res.get('upcoming_calendar', []))} upcoming.")
    except Exception as e:
        print(f"FAILED: FRED fetch failed: {e}")

def test_sentiment():
    print("Testing Sentiment (Alpha Vantage)...")
    try:
        sent = SentimentData()
        res = sent.fetch()
        if 'Error' in res.get('news_sentiment', {}):
            print(f"FAILED: {res['news_sentiment']['Error']}")
        else:
            print(f"SUCCESS: Sentiment: {res.get('news_sentiment', {}).get('Sentiment_Label')}")
    except Exception as e:
        print(f"FAILED: Sentiment fetch failed: {e}")

def test_schwab():
    print("Testing Schwab...")
    try:
        schwab = SchwabMarketData()
        if not schwab.client:
            print("FAILED: Schwab client not initialized (check token path and credentials).")
            return
        price = schwab.get_price("SPY")
        print(f"SUCCESS: SPY Price from Schwab: {price}")
    except Exception as e:
        print(f"FAILED: Schwab fetch failed: {e}")

if __name__ == "__main__":
    print("Starting tests...")
    test_news()
    test_fred()
    test_sentiment()
    test_schwab()
