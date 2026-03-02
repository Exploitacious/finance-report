import os
import requests
import time
from .base import DataSource
from app.config import PRIMARY_SYMBOL

class SentimentData(DataSource):
    def fetch(self):
        symbol = PRIMARY_SYMBOL
        data = {}
        # Retry logic handled inside _analyze_sentiment
        data['news_sentiment'] = self._analyze_sentiment(symbol)
        return data

    def _analyze_sentiment(self, symbol):
        key = os.getenv("ALPHA_VANTAGE_API_KEY")
        if not key: return {}
        
        url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={symbol}&apikey={key}"
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    
                    # Check for API Limit message
                    if 'Information' in data and 'rate limit' in data['Information']:
                        print(f"Alpha Vantage Rate Limit Hit. Attempt {attempt+1}/{max_retries}")
                        if attempt < max_retries - 1:
                            time.sleep(2) # Wait 2 seconds before retry
                            continue
                        else:
                            return {'Error': "Daily Rate Limit Exceeded"}

                    if 'feed' in data:
                        scores = [float(f['overall_sentiment_score']) for f in data['feed'][:10]]
                        avg_score = sum(scores) / len(scores) if scores else 0
                        return {
                            'Average_Sentiment_Score': round(avg_score, 2),
                            'Sentiment_Label': "Bullish" if avg_score > 0.15 else ("Bearish" if avg_score < -0.15 else "Neutral"),
                            'Articles_Analyzed': len(scores)
                        }
            except Exception as e:
                print(f"Error fetching sentiment: {e}")
            
            time.sleep(1) # Base delay
            
        return {'Error': "Sentiment Data Unavailable"}
