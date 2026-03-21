import os
import anthropic
from dotenv import load_dotenv
from pipeline.db.supabase_client import supabase

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

THREAD_PROMPT = """
You are an expert in tech and AI communication on X (Twitter).
Your style is direct, informative and with your own personality.
You never sound like a bot or a corporate press release.
You use clear language, strategic emojis, and always add your own perspective.
"""

def format_as_thread(title: str, content: str, url: str) -> list[str]:
    """
    Dado un post, genera un hilo de X listo para publicar.
    Devuelve lista de tweets (máximo 5).
    """
    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1000,
            system=THREAD_PROMPT,
            messages=[{
                "role": "user",
                "content": f"""Generate an X (Twitter) thread based on this content.
TITLE: {title}
CONTENT: {content[:800]}
SOURCE URL: {url}

STRICT RULES:
- Between 3 and 5 tweets
- Each tweet maximum 270 characters
- Tweet 1: strong hook that generates curiosity, include 🧵
- Tweets 2-4: development with your own perspective, not just summarizing
- Last tweet: conclusion + source with the URL
- No hashtags
- Separate each tweet with the line: ---TWEET---

Respond ONLY with the tweets separated by ---TWEET---, no numbering or extra text."""
            }]
        )

        raw = response.content[0].text.strip()
        tweets = [t.strip() for t in raw.split("---TWEET---") if t.strip()]

        # Validar longitud de cada tweet
        validated = []
        for tweet in tweets:
            if len(tweet) <= 280:
                validated.append(tweet)
            else:
                # Truncar si se pasa
                validated.append(tweet[:277] + "...")

        return validated[:5]  # máximo 5 tweets

    except Exception as e:
        print(f"  ❌ Error generando hilo: {e}")
        return []


def process_pending_posts(limit: int = 5):
    """
    Coge los mejores posts pendientes y genera su hilo de X.
    """
    print("✍️  Iniciando reformateo de contenido...")

    posts = (
        supabase.table("posts")
        .select("*")
        .eq("status", "pending")
        .not_.is_("semantic_score", "null")
        .gte("semantic_score", 0.6)
        .order("score", desc=True)
        .limit(limit)
        .execute()
        .data
    )

    if not posts:
        print("  No hay posts listos para reformatear")
        return []

    print(f"  Procesando {len(posts)} posts...")
    results = []

    for post in posts:
        print(f"\n  📝 {post['title'][:60]}...")
        tweets = format_as_thread(
            post["title"],
            post["content"] or "",
            post["url"]
        )

        if not tweets:
            continue

        # Guardar hilo en Supabase
        supabase.table("posts").update({
            "thread": tweets,
            "status": "ready"
        }).eq("id", post["id"]).execute()

        print(f"  ✅ Hilo generado ({len(tweets)} tweets)")
        for i, tweet in enumerate(tweets, 1):
            print(f"\n  [{i}] {tweet}")
            print(f"  chars: {len(tweet)}")

        results.append({"post": post, "thread": tweets})

    print(f"\n📊 Total hilos generados: {len(results)}")
    return results


if __name__ == "__main__":
    process_pending_posts()