import httpx
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from pipeline.db.supabase_client import save_post

load_dotenv()

SUBREDDITS = [
    "artificial",
    "MachineLearning",
    "ChatGPT",
    "OpenAI",
    "technology"
]

HEADERS = {
    "User-Agent": "social-amplifier/0.1"
}

def calculate_score(upvotes: int, comments: int, created_utc: float) -> dict:
    now = datetime.now(timezone.utc).timestamp()
    hours_since = max((now - created_utc) / 3600, 0.1)
    raw = upvotes + comments * 1.5
    viral_score = round(raw / hours_since, 2)

    return {
        "viral_score": viral_score,
        "upvotes": upvotes,
        "comments": comments,
        "hours_since": round(hours_since, 2)
    }

def scrape_subreddit(subreddit_name: str, limit: int = 25):
    print(f"  Scraping r/{subreddit_name}...")
    saved = 0

    try:
        url = f"https://www.reddit.com/r/{subreddit_name}/top.json?t=day&limit={limit}"
        response = httpx.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        posts = response.json()["data"]["children"]

        for item in posts:
            post = item["data"]

            if len(post["title"]) < 20:
                continue

            score_data = calculate_score(
                post["ups"],
                post["num_comments"],
                post["created_utc"]
            )

            if score_data["viral_score"] < 50:
                continue

            content = post["selftext"] if post["selftext"] else post["title"]

            post_data = {
                "source": f"reddit/r/{subreddit_name}",
                "external_id": f"reddit_{post['id']}",
                "title": post["title"],
                "content": content[:2000],
                "url": f"https://reddit.com{post['permalink']}",
                "author": post["author"],
                "score": score_data["viral_score"],
                "raw_score": score_data,
                "status": "pending"
            }

            if save_post(post_data):
                saved += 1

    except Exception as e:
        print(f"  ❌ Error en r/{subreddit_name}: {e}")

    return saved

def run():
    print("🚀 Iniciando scraper de Reddit...")
    total = 0

    for subreddit in SUBREDDITS:
        saved = scrape_subreddit(subreddit)
        total += saved
        print(f"  ✅ r/{subreddit}: {saved} posts guardados")

    print(f"\n📊 Total: {total} posts nuevos en Supabase")
    return total

if __name__ == "__main__":
    run()