import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

def save_post(post: dict) -> bool:
    """Guarda un post en Supabase, ignora duplicados."""
    try:
        supabase.table("posts").upsert(
            post,
            on_conflict="external_id"
        ).execute()
        return True
    except Exception as e:
        print(f"Error guardando post: {e}")
        return False

def get_pending_posts(limit: int = 10):
    """Recupera posts pendientes ordenados por score."""
    return (
        supabase.table("posts")
        .select("*")
        .eq("status", "pending")
        .order("score", desc=True)
        .limit(limit)
        .execute()
        .data
    )