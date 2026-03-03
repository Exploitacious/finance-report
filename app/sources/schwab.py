import os
import json
import pandas as pd
from schwab.auth import client_from_token_file
from schwab.client import Client
from ..config import SCHWAB_TOKEN_FILE

class SchwabMarketData:
    def __init__(self, token_path=None):
        # Use config or override from constructor
        self.token_path = token_path or SCHWAB_TOKEN_FILE
        self.client = self._init_client()

    def _init_client(self):
        try:
            key = os.getenv('SCHWAB_API_KEY')
            secret = os.getenv('SCHWAB_API_SECRET')
            
            if not key or not secret:
                print("CRITICAL ERROR: Schwab API key or secret not found in environment. Halting Schwab ingestion.")
                return None
            
            if not os.path.exists(self.token_path):
                print(f"CRITICAL ERROR: Schwab token file not found at {self.token_path}. Halting Schwab ingestion.")
                return None

            # Use enforce_enums=False for better string/raw data robustness
            return client_from_token_file(self.token_path, key, secret, enforce_enums=False)
        except Exception as e:
            print(f"CRITICAL ERROR: Failed to initialize Schwab client: {e}. Halting Schwab ingestion.")
            return None

    def _normalize_symbol(self, symbol):
        # Map yfinance-style or raw ticker to Schwab-style
        if symbol == 'DX-Y.NYB' or symbol == 'DXY':
            return '$DXY'
        if symbol.startswith('^'):
            return symbol.replace('^', '$')
        if symbol in ['VIX', 'TNX', 'IRX', 'SPX', 'NDX', 'RUT']:
            return f"${symbol}"
        return symbol

    def get_quote(self, symbol):
        if not self.client: return None
        try:
            sym = self._normalize_symbol(symbol)
            resp = self.client.get_quote(sym)
            if resp.status_code == 200:
                data = resp.json()
                if sym in data:
                    return data.get(sym)
                # Fallback to case-insensitive check
                for k, v in data.items():
                    if k.upper() == sym.upper():
                        return v
            return None
        except Exception:
            return None

    def get_quotes(self, symbols):
        if not self.client: return None
        try:
            s_syms = [self._normalize_symbol(s) for s in symbols]
            resp = self.client.get_quotes(s_syms)
            if resp.status_code == 200:
                data = resp.json()
                res = {}
                for i, s in enumerate(symbols):
                    norm = s_syms[i]
                    if norm in data:
                        res[s] = data[norm]
                return res
            return None
        except Exception:
            return None

    def get_price(self, symbol):
        quote = self.get_quote(symbol)
        if quote:
            return quote.get('quote', {}).get('lastPrice')
        return None

    def get_prices(self, symbols):
        quotes = self.get_quotes(symbols)
        if quotes:
            return {sym: q.get('quote', {}).get('lastPrice') if q else None for sym, q in quotes.items()}
        return {}

    def get_history(self, symbol, period='1y'):
        if not self.client: return pd.DataFrame()
        
        try:
            sym = self._normalize_symbol(symbol)
            
            # Use strings if Enums fail or if enforce_enums=False is set
            resp = self.client.get_price_history(
                sym,
                period_type=Client.PriceHistory.PeriodType.YEAR,
                period=Client.PriceHistory.Period.ONE_YEAR,
                frequency_type=Client.PriceHistory.FrequencyType.DAILY,
                frequency=Client.PriceHistory.Frequency.DAILY
            )
            
            if resp.status_code == 200:
                data = resp.json()
                candles = data.get('candles', [])
                if not candles: return pd.DataFrame()
                
                df = pd.DataFrame(candles)
                df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
                df.set_index('datetime', inplace=True)
                df.rename(columns={'close': 'Close', 'open': 'Open', 'high': 'High', 'low': 'Low', 'volume': 'Volume'}, inplace=True)
                return df[['Close', 'Open', 'High', 'Low', 'Volume']]
            return pd.DataFrame()
        except Exception as e:
            print(f"Error fetching history for {symbol}: {e}")
            return pd.DataFrame()

    def get_option_chain(self, symbol, **kwargs):
        if not self.client: return None
        try:
            sym = self._normalize_symbol(symbol)
            resp = self.client.get_option_chain(sym, **kwargs)
            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception:
            return None
