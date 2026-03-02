import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
from .base import DataSource
from app.config import TICKERS, WATCH_LIST

class MarketData(DataSource):
    def __init__(self, schwab_source=None):
        super().__init__()
        self.schwab = schwab_source

    def fetch(self):
        data = {}
        data['macro'] = self._analyze_macro()
        data['volatility'] = self._analyze_volatility()
        data['relative_strength'] = self._analyze_relative_strength()
        data['calendar'] = self._fetch_calendar()
        
        # Boards as requested
        equity_tickers = ['SPY', 'QQQ', 'IWM']
        macro_tickers = ['VIX', 'TNX', 'GLD', 'DXY', 'SLV']
        
        data['equity_board'] = {}
        for ticker in equity_tickers:
            symbol = TICKERS.get(ticker, ticker)
            data['equity_board'][ticker] = self._analyze_single_ticker(symbol, ticker)
            
        data['macro_board'] = {}
        for ticker in macro_tickers:
            symbol = TICKERS.get(ticker, ticker)
            data['macro_board'][ticker] = self._analyze_single_ticker(symbol, ticker)
            
        return data

    def _get_price(self, ticker):
        if self.schwab and ticker != 'DX-Y.NYB':
            return self.schwab.get_price(ticker)
        try:
            hist = yf.Ticker(ticker).history(period='1d')
            if not hist.empty:
                return hist['Close'].iloc[-1]
        except:
            pass
        return None

    def _get_history(self, ticker, period='1y'):
        if self.schwab and ticker != 'DX-Y.NYB':
            return self.schwab.get_history(ticker, period=period)
        try:
            return yf.Ticker(ticker).history(period=period)
        except:
            return pd.DataFrame()

    def _analyze_single_ticker(self, symbol, key):
        res = {'Symbol': symbol}
        
        # 1. Price
        price = self._get_price(symbol)
        if price is None:
            hist = self._get_history(symbol)
            if not hist.empty:
                price = hist['Close'].iloc[-1]
                res['Price'] = price
            else:
                res['Error'] = "No Data"
                return res
        else:
            res['Price'] = price
            hist = self._get_history(symbol)

        # 2. Technicals & IVR
        if not hist.empty and len(hist) > 20:
            try:
                # Basic Technicals
                res['SMA_50'] = hist['Close'].rolling(window=50).mean().iloc[-1]
                res['SMA_200'] = hist['Close'].rolling(window=200).mean().iloc[-1]
                
                delta = hist['Close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                res['RSI_14'] = 100 - (100 / (1 + rs.iloc[-1]))
                
                if res['SMA_50']:
                    res['Distance_from_50DMA'] = (price - res['SMA_50']) / res['SMA_50']
                
                # IVR (Implied Volatility Rank) Approximation
                # We use 52-week HV as a proxy if IV history is unavailable
                returns = np.log(hist['Close'] / hist['Close'].shift(1))
                hv = returns.rolling(window=21).std() * np.sqrt(252) * 100
                if not hv.dropna().empty:
                    current_hv = hv.iloc[-1]
                    hv_min = hv.min()
                    hv_max = hv.max()
                    if hv_max > hv_min:
                        res['IVR'] = round(((current_hv - hv_min) / (hv_max - hv_min)) * 100, 2)
            except Exception:
                pass

        # 3. GEX & ZGL (Only for Derivatives Board)
        if key in ['SPY', 'QQQ', 'IWM', 'GLD', 'SLV']:
            gex_data = self._calculate_gex_and_zgl(symbol, price)
            if gex_data:
                res.update(gex_data)
                
        return res

    def _calculate_gex_and_zgl(self, symbol, spot):
        if not self.schwab: return None
        
        try:
            # Request more strikes and all expirations to get a better profile
            # Schwab-py usually handles these as kwargs
            chain = self.schwab.get_option_chain(symbol, strike_count=50, strike_range='NEAR_THE_MONEY')
            if not chain or 'callExpDateMap' not in chain: return None
            
            total_call_gex = 0
            total_put_gex = 0
            strike_gamma = {} # strike: net_gamma
            
            # Helper to process maps
            def process_map(exp_map, is_call):
                nonlocal total_call_gex, total_put_gex
                # Take first 5 expirations (about 1 month of data)
                target_dates = sorted(exp_map.keys())[:5]
                for date_key in target_dates:
                    strikes_map = exp_map.get(date_key, {})
                    for strike, options in strikes_map.items():
                        try:
                            s_val = float(strike)
                            for opt in options:
                                gamma = opt.get('gamma', 0)
                                oi = opt.get('openInterest', 0)
                                # GEX weight: Gamma * OI
                                gex_val = gamma * oi
                                if is_call:
                                    total_call_gex += gex_val
                                    strike_gamma[s_val] = strike_gamma.get(s_val, 0) + gex_val
                                else:
                                    total_put_gex += gex_val
                                    strike_gamma[s_val] = strike_gamma.get(s_val, 0) - gex_val
                        except: continue

            process_map(chain.get('callExpDateMap', {}), True)
            process_map(chain.get('putExpDateMap', {}), False)
            
            if not strike_gamma: return None
            
            # PCR calculation
            pcr = total_put_gex / total_call_gex if total_call_gex > 0 else 0
            
            # ZGL: Price where net gamma flips sign
            sorted_strikes = sorted(strike_gamma.keys())
            zgl = None
            for i in range(len(sorted_strikes) - 1):
                s1, s2 = sorted_strikes[i], sorted_strikes[i+1]
                g1, g2 = strike_gamma[s1], strike_gamma[s2]
                if g1 * g2 <= 0: # Sign flip
                    # Linear interpolation: s = s1 + (0 - g1) * (s2 - s1) / (g2 - g1)
                    if g2 != g1:
                        zgl = s1 - g1 * (s2 - s1) / (g2 - g1)
                    else:
                        zgl = s1
                    break
            
            # Fallback to closest if no flip (though usually there is one near ATM)
            if zgl is None:
                zgl = min(strike_gamma.keys(), key=lambda s: abs(strike_gamma[s]))
            
            return {
                'PCR_OI': round(pcr, 2),
                'GEX_Sentiment': "Bearish" if pcr > 1.2 else ("Bullish" if pcr < 0.7 else "Neutral"),
                'ZGL': round(zgl, 2)
            }
        except Exception:
            return None

    def _analyze_macro(self):
        tickers = TICKERS
        res = {}
        tnx = self._get_price(tickers['TNX'])
        irx = self._get_price(tickers['IRX'])
        
        res['TNX_10Y'] = tnx
        res['IRX_13W'] = irx
        res['DXY'] = self._get_price(tickers['DXY'])
        
        if tnx and irx:
            try:
                tnx_f, irx_f = float(tnx), float(irx)
                tnx_yield = tnx_f / 10.0 if tnx_f > 10 else tnx_f
                irx_yield = irx_f / 10.0 if irx_f > 10 else irx_f
                res['Yield_Curve_Spread'] = tnx_yield - irx_yield
                res['Curve_Status'] = "Inverted" if (tnx_yield - irx_yield) < 0 else "Normal"
            except: pass

        hyg = self._get_price(tickers['HYG'])
        lqd = self._get_price(tickers['LQD'])
        if hyg and lqd:
            try:
                ratio = float(hyg) / float(lqd)
                res['Credit_Risk_Ratio'] = ratio
                res['Credit_Signal'] = "Risk ON" if ratio > 0.65 else "Risk OFF"
            except: pass
        return res

    def _analyze_volatility(self):
        tickers = TICKERS
        res = {}
        vix = self._get_price(tickers['VIX'])
        vix3m = self._get_price(tickers['VIX3M'])
        res['VIX'] = vix
        res['VIX3M'] = vix3m
        if vix and vix3m:
            try:
                spread = float(vix3m) - float(vix)
                res['Term_Structure'] = "Contango (Normal)" if spread > 0 else "Backwardation (Panic)"
            except: pass
        return res

    def _analyze_relative_strength(self):
        tickers = TICKERS
        res = {}
        qqq = self._get_price(tickers['QQQ'])
        spy = self._get_price(tickers['SPY'])
        if qqq and spy:
            try: res['Growth_vs_Value_Ratio'] = float(qqq) / float(spy)
            except: pass
        xly = self._get_price(tickers['XLY'])
        xlp = self._get_price(tickers['XLP'])
        if xly and xlp:
            try:
                ratio = float(xly) / float(xlp)
                res['Risk_Appetite_Ratio'] = ratio
                res['Rotation_Signal'] = "Risk ON" if ratio > 2.3 else "Defensive"
            except: pass
        return res

    def _fetch_calendar(self):
        drivers = WATCH_LIST
        upcoming = []
        today = datetime.now().date()
        for d in drivers:
            try:
                tk = yf.Ticker(d)
                cal = tk.calendar
                if isinstance(cal, dict) and 'Earnings Date' in cal:
                    dates = cal['Earnings Date']
                    if isinstance(dates, list) and len(dates) > 0:
                        e_date = dates[0]
                        if hasattr(e_date, 'date'): e_date = e_date.date()
                        if e_date >= today:
                            upcoming.append({'ticker': d, 'date': e_date.isoformat()})
            except: continue
        return sorted(upcoming, key=lambda x: x['date'])
