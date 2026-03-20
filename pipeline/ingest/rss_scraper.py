import feedparser
import httpx
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from pipeline.db.supabase_client import save_post

load_dotenv()

FEEDS = {
    "tldr_ai": "https://tldr.tech/api/rss/ai",
    "hacker_news": "https://news.ycombinator.com/rss",
    "mit_ai": "https://news.mit.edu/rss/topic/artificial-intelligence2",
    "venturebeat_ai": "https://venturebeat.com/category/ai/feed/",
    "the_rundown_ai": "https://www.therundown.ai/rss",
}

def parse_date(entry) -> float:
    """Extrae timestamp del post, devuelve ahora si no existe."""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).timestamp()
    return datetime.now(timezone.utc).timestamp()

def calculate_score(created_utc: float) -> dict:
    """
    RSS no tiene métricas de engagement.
    El score se basa únicamente en la frescura del contenido.
    """
    now = datetime.now(timezone.utc).timestamp()
    hours_since = max((now - created_utc) / 3600, 0.1)

    # Posts de menos de 6h tienen score alto, luego decae
    freshness_score = round(500 / hours_since, 2)

    return {
        "viral_score": freshness_score,
        "hours_since": round(hours_since, 2),
        "source_type": "rss"
    }

def scrape_feed(feed_name: str, feed_url: str):
    """Scrape de un feed RSS."""
    print(f"  Scraping {feed_name}...")
    saved = 0

    try:
        feed = feedparser.parse(feed_url)

        for entry in feed.entries[:15]:  # máximo 15 por feed
            title = entry.get("title", "")
            content = entry.get("summary", entry.get("description", ""))
            url = entry.get("link", "")

            if not title or not url:
                continue

            if len(title) < 20:
                continue

            created_utc = parse_date(entry)
            score_data = calculate_score(created_utc)

            # Solo posts de las últimas 48h
            if score_data["hours_since"] > 48:
                continue

            # Generar id único a partir de la url
            external_id = f"rss_{feed_name}_{hash(url)}"

            post_data = {
                "source": f"rss/{feed_name}",
                "external_id": external_id,
                "title": title,
                "content": content[:2000],
                "url": url,
                "author": feed_name,
                "score": score_data["viral_score"],
                "raw_score": score_data,
                "status": "pending"
            }

            if save_post(post_data):
                saved += 1

    except Exception as e:
        print(f"  ❌ Error en {feed_name}: {e}")

    return saved

def run():
    print("🚀 Iniciando scraper de RSS...")
    total = 0

    for feed_name, feed_url in FEEDS.items():
        saved = scrape_feed(feed_name, feed_url)
        total += saved
        print(f"  ✅ {feed_name}: {saved} posts guardados")

    print(f"\n📊 Total: {total} posts nuevos en Supabase")
    return total

if __name__ == "__main__":
    run()