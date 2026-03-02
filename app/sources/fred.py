import os
import requests
from datetime import datetime, timedelta
from .base import DataSource

class FredSource(DataSource):
    def fetch(self):
        data = {}
        # 1. Fetch today's prints
        data['todays_prints'] = self._fetch_todays_prints()
        # 2. Fetch upcoming calendar for next 30 days (Major Releases Only)
        data['upcoming_calendar'] = self._fetch_upcoming_calendar()
        return data

    def _fetch_todays_prints(self):
        key = os.getenv("FRED_API_KEY")
        if not key: return []
        
        today = datetime.now().strftime("%Y-%m-%d")
        prints = []
        
        try:
            # Get today's releases
            url = f"https://api.stlouisfed.org/fred/releases?api_key={key}&file_type=json&realtime_start={today}"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                releases = resp.json().get('releases', [])
                # Take top 5 releases to avoid too many requests
                for r in releases[:5]:
                    r_id = r['id']
                    # Get the most popular series for this release
                    s_url = f"https://api.stlouisfed.org/fred/release/series?release_id={r_id}&api_key={key}&file_type=json&order_by=popularity&sort_order=desc"
                    s_resp = requests.get(s_url, timeout=10)
                    if s_resp.status_code == 200:
                        series_list = s_resp.json().get('seriess', [])
                        if series_list:
                            s = series_list[0]
                            s_id = s['id']
                            # Get the observation released today
                            o_url = f"https://api.stlouisfed.org/fred/series/observations?series_id={s_id}&api_key={key}&file_type=json&realtime_start={today}"
                            o_resp = requests.get(o_url, timeout=10)
                            if o_resp.status_code == 200:
                                obs = o_resp.json().get('observations', [])
                                if obs:
                                    prints.append({
                                        'release_name': r['name'],
                                        'series_title': s['title'],
                                        'value': obs[-1]['value'],
                                        'date': obs[-1]['date']
                                    })
        except Exception:
            pass
        return prints

    def _fetch_upcoming_calendar(self):
        key = os.getenv("FRED_API_KEY")
        if not key: return []
        
        # Major Economic Indicators IDs
        # 10: CPI, 53: GDP, 50: Employment Situation, 46: PPI, 9: Retail Sales, 13: Industrial Production
        major_ids = {
            10: "Consumer Price Index (CPI)",
            53: "Gross Domestic Product (GDP)",
            50: "Employment Situation (Payrolls)",
            46: "Producer Price Index (PPI)",
            9: "Retail Sales",
            13: "Industrial Production"
        }
        
        upcoming = []
        today = datetime.now()
        compare_tomorrow = (today + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        one_month_later = today + timedelta(days=30)
        
        try:
            for rid, name in major_ids.items():
                # Fetch dates for this specific release
                # We sort descending to get future dates first (2026, 2027...)
                url = f"https://api.stlouisfed.org/fred/release/dates?release_id={rid}&api_key={key}&file_type=json&include_release_dates_with_no_data=true&sort_order=desc&limit=10"
                
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    dates = resp.json().get('release_dates', [])
                    for item in dates:
                        try:
                            r_date = datetime.strptime(item['date'], "%Y-%m-%d")
                            if compare_tomorrow <= r_date <= one_month_later:
                                upcoming.append({
                                    'release_name': name,
                                    'date': item['date']
                                })
                        except (ValueError, KeyError):
                            continue
                            
            # Sort all upcoming events by date
            upcoming.sort(key=lambda x: x['date'])
            return upcoming
            
        except Exception:
            pass
        return []
