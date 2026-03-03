import httpx
import xml.etree.ElementTree as ET
import json
import os
from pathlib import Path
from .base import DataSource
from ..config import DATA_DIR

class NewsData(DataSource):
    def fetch(self):
        # Use configurable data directory from config
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        state_file = DATA_DIR / ".news_state.json"
        
        last_pub_date = self._load_state(state_file)
        
        feed_url = "https://www.fxstreet.com/rss"
        news_items, newest_pub_date = self._fetch_rss(feed_url, last_pub_date)
        
        if newest_pub_date:
            self._save_state(state_file, newest_pub_date)
            
        return {
            'fx_news': self._format_news(news_items)
        }

    def _load_state(self, state_file):
        if state_file.exists():
            try:
                with open(state_file, 'r') as f:
                    return json.load(f).get('last_pub_date')
            except Exception:
                pass
        return None

    def _save_state(self, state_file, last_pub_date):
        try:
            with open(state_file, 'w') as f:
                json.dump({'last_pub_date': last_pub_date}, f)
        except Exception as e:
            print(f"Error saving news state: {e}")

    def _fetch_rss(self, url, last_pub_date):
        news_items = []
        newest_date_str = last_pub_date
        
        # Parse the last_pub_date if it exists
        last_dt = None
        if last_pub_date:
            try:
                from dateutil import parser
                last_dt = parser.parse(last_pub_date)
            except Exception:
                pass

        try:
            with httpx.Client(follow_redirects=True, timeout=10.0) as client:
                response = client.get(url)
                response.raise_for_status()
                
                root = ET.fromstring(response.content)
                items = root.findall('.//item')
                
                # Iterate through all items
                for item in items:
                    title = item.find('title').text if item.find('title') is not None else "No Title"
                    link = item.find('link').text if item.find('link') is not None else ""
                    pub_date_str = item.find('pubDate').text if item.find('pubDate') is not None else ""
                    
                    try:
                        from dateutil import parser
                        current_dt = parser.parse(pub_date_str)
                        
                        # If we have a last_dt, only include items strictly NEWER than it
                        if last_dt and current_dt <= last_dt:
                            continue
                            
                        news_items.append({
                            'title': title,
                            'link': link,
                            'pub_date': pub_date_str,
                            'dt': current_dt # Store for sorting
                        })
                    except Exception:
                        continue # Skip items with unparseable dates

                # Sort by date descending (newest first)
                news_items.sort(key=lambda x: x['dt'], reverse=True)
                
                # Update newest_date_str to the very latest item found (if any)
                if news_items:
                    newest_date_str = news_items[0]['pub_date']
                elif not news_items and not last_pub_date and items:
                     # If it's the first run ever and we found items, take the newest one from the feed
                     # (even though we are returning all of them)
                     try:
                         from dateutil import parser
                         first_dt = parser.parse(items[0].find('pubDate').text)
                         newest_date_str = items[0].find('pubDate').text
                     except:
                         pass

        except Exception as e:
            print(f"Error fetching RSS: {e}")
            
        return news_items, newest_date_str

    def _format_news(self, items):
        if not items:
            return "No new articles found since last report."
            
        output = f"Unread articles ({len(items)}):\n\n"
        for i, item in enumerate(items, 1):
            output += f"  [{i}] {item['title']}\n"
            output += f"       URL: {item['link']}\n"
            output += f"       Published: {item['pub_date']}\n\n"
        return output
