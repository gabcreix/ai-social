import os
import httpx
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from pipeline.db.supabase_client import supabase

load_dotenv()

BUFFER_TOKEN = os.getenv("BUFFER_TOKEN")
BUFFER_CHANNEL_ID = os.getenv("BUFFER_PROFILE_ID")
BUFFER_API = "https://api.buffer.com"

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {BUFFER_TOKEN}"
}


def enqueue_tweet(text: str, due_at: str) -> dict:
    """Encola un tweet en Buffer via GraphQL."""
    mutation = """
    mutation CreatePost {
      createPost(input: {
        text: "%s",
        channelId: "%s",
        schedulingType: automatic,
        mode: customScheduled,
        dueAt: "%s"
      }) {
        ... on PostActionSuccess {
          post {
            id
            text
          }
        }
        ... on MutationError {
          message
        }
      }
    }
    """ % (text.replace('"', '\\"').replace('\n', '\\n'), BUFFER_CHANNEL_ID, due_at)

    response = httpx.post(
        BUFFER_API,
        headers=HEADERS,
        json={"query": mutation},
        timeout=10
    )

    print(f"  Status: {response.status_code}")

    if not response.text.strip():
        return {"success": False, "error": "empty response"}

    data = response.json()

    if "errors" in data:
        return {"success": False, "error": data["errors"]}

    result = data.get("data", {}).get("createPost", {})

    if "post" in result:
        return {"success": True, "id": result["post"]["id"]}
    elif "message" in result:
        return {"success": False, "error": result["message"]}

    return {"success": False, "error": "unknown response", "raw": data}


def enqueue_thread(tweets: list, post_id: str) -> bool:
    """Encola un hilo completo en Buffer."""
    print(f"  📤 Encolando hilo de {len(tweets)} tweets...")
    success = True

    for i, tweet in enumerate(tweets, 1):
        due_at = (datetime.now(timezone.utc) + timedelta(minutes=i * 2)).isoformat()
        result = enqueue_tweet(tweet, due_at)

        if result.get("success"):
            print(f"  ✅ Tweet {i}/{len(tweets)} encolado")
        else:
            print(f"  ❌ Error en tweet {i}: {result.get('error')}")
            success = False

    if success:
        supabase.table("posts").update({
            "status": "published"
        }).eq("id", post_id).execute()
        print(f"  ✅ Estado actualizado a 'published'")

    return success


def process_ready_posts(limit: int = 3):
    """Coge posts con estado 'ready' y los envía a Buffer."""
    print("📤 Iniciando publicación en Buffer...")

    posts = (
        supabase.table("posts")
        .select("*")
        .eq("status", "ready")
        .order("score", desc=True)
        .limit(limit)
        .execute()
        .data
    )

    if not posts:
        print("  No hay posts listos para publicar")
        return

    print(f"  Encontrados {len(posts)} posts listos")

    for post in posts:
        print(f"\n  📝 {post['title'][:60]}...")
        thread = post.get("thread")

        if not thread:
            print("  ⚠️ Sin hilo generado, saltando")
            continue

        enqueue_thread(thread, post["id"])

    print("\n✅ Publicación completada")


if __name__ == "__main__":
    process_ready_posts()