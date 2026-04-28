import asyncio
import json
import random
import os
from bs4 import BeautifulSoup
import httpx
import logging
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
]

# Can be overridden later to route through a Cloudflare Worker
PROXY_URL = None

async def fetch_reddit_page(client: httpx.AsyncClient, subreddit: str) -> str:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    
    url = f"https://old.reddit.com/r/{subreddit}/top/?sort=top&t=day"
    if PROXY_URL:
        # Example of how proxy routing could work
        url = f"{PROXY_URL}?url={url}"

    try:
        response = await client.get(url, headers=headers, follow_redirects=True)
        response.raise_for_status()
        return response.text
    except httpx.HTTPError as e:
        logging.error(f"HTTP error occurred while fetching {subreddit}: {e}")
        return ""
    except Exception as e:
        logging.error(f"An unexpected error occurred while fetching {subreddit}: {e}")
        return ""

def parse_html(html: str) -> list:
    results = []
    if not html:
        return results

    soup = BeautifulSoup(html, 'html.parser')
    # Using old.reddit.com structure as it's often easier to scrape without JS
    things = soup.find_all('div', class_='thing')
    
    for thing in things:
        title_elem = thing.find('p', class_='title')
        score_elem = thing.find('div', class_='score unvoted')
        
        if title_elem and score_elem:
            title = title_elem.text.strip()
            score_text = score_elem.text.strip()
            
            # Handle hidden scores or empty strings
            if score_text and score_text != '•':
                try:
                    # '1.5k' -> 1500
                    if 'k' in score_text:
                        score = int(float(score_text.replace('k', '')) * 1000)
                    else:
                        score = int(score_text)
                except ValueError:
                    score = 0
            else:
                score = 0
                
            results.append({
                "title": title,
                "upvotes": score
            })
            
    return results

async def main():
    subreddits = ["sidehustle", "SaaS"]

    supabase_url: str = os.environ.get("SUPABASE_URL")
    supabase_key: str = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        logging.error("Supabase credentials not found in environment variables.")
        return

    supabase: Client = create_client(supabase_url, supabase_key)

    # HTTP/2 support is crucial for modern scraping
    async with httpx.AsyncClient(http2=True) as client:
        for subreddit in subreddits:
            logging.info(f"Fetching data for r/{subreddit}...")
            html = await fetch_reddit_page(client, subreddit)
            data = parse_html(html)
            
            if data:
                # Prepare records for Supabase
                records = []
                for item in data:
                    records.append({
                        "post_title": item["title"],
                        "engagement_score": item["upvotes"],
                        "source_platform": "reddit",
                        "target_niche": subreddit
                    })
                
                try:
                    supabase.table("raw_market_signals").insert(records).execute()
                    logging.info(f"Inserted {len(records)} records for r/{subreddit} into Supabase.")
                except Exception as e:
                    logging.error(f"Failed to insert records for r/{subreddit}: {e}")
            
            # Random delay to mimic human behavior
            await asyncio.sleep(random.uniform(1.5, 3.5))

    logging.info("Scraping and ingestion complete.")

if __name__ == "__main__":
    asyncio.run(main())
