from pipeline.ingest.reddit_scraper import run as run_reddit
from pipeline.ingest.rss_scraper import run as run_rss

def run():
    print("=" * 40)
    print("  SOCIAL AMPLIFIER - INGESTA")
    print("=" * 40)

    reddit_total = run_reddit()
    print()
    rss_total = run_rss()

    print()
    print("=" * 40)
    print(f"  TOTAL POSTS NUEVOS: {reddit_total + rss_total}")
    print("=" * 40)

if __name__ == "__main__":
    run()