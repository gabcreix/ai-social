import os
import anthropic
from dotenv import load_dotenv
from pipeline.db.supabase_client import supabase

load_dotenv()

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

NICHE_CONTEXT = """
Eres un filtro de contenido especializado en IA y tecnología.
Tu única tarea es puntuar si un post es relevante para una audiencia
interesada en: inteligencia artificial, machine learning, modelos de lenguaje,
herramientas de IA, startups tech, investigación en IA, automatización,
y tendencias tecnológicas.
"""

def score_relevance(title: str, content: str) -> dict:
    try:
        text = f"Título: {title}\nContenido: {content[:500]}"

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=100,
            system=NICHE_CONTEXT,
            messages=[{
                "role": "user",
                "content": f"""Puntúa este post del 0.0 al 1.0 según su relevancia para el nicho IA/Tech.

{text}

Responde SOLO con este formato JSON, sin texto adicional, sin markdown, sin backticks:
{{"score": 0.0, "reason": "motivo breve"}}"""
            }]
        )

        import json
        import re

        raw = response.content[0].text.strip()

        # Extraer JSON aunque venga con texto extra
        match = re.search(r'\{.*?\}', raw, re.DOTALL)
        if not match:
            raise ValueError(f"No JSON encontrado en respuesta: {raw}")

        result = json.loads(match.group())
        return {
            "relevance_score": float(result["score"]),
            "reason": result["reason"]
        }

    except Exception as e:
        print(f"  ❌ Error scoring semántico: {e}")
        return {"relevance_score": 0.5, "reason": "error, score neutro"}


def run(min_relevance: float = 0.6):
    """
    Procesa posts pendientes y actualiza su score combinado.
    Descarta posts por debajo del umbral de relevancia.
    """
    print("🧠 Iniciando scoring semántico...")

    # Obtener posts pendientes sin score semántico
    posts = (
        supabase.table("posts")
        .select("*")
        .eq("status", "pending")
        .is_("semantic_score", "null")
        .limit(20)
        .execute()
        .data
    )

    if not posts:
        print("  No hay posts pendientes para puntuar")
        return

    print(f"  Procesando {len(posts)} posts...")

    approved = 0
    rejected = 0

    for post in posts:
        result = score_relevance(post["title"], post["content"] or "")
        relevance = result["relevance_score"]

        print(f"  [{relevance:.2f}] {post['title'][:60]}...")

        # Score final combinado: viral * relevancia
        final_score = round(post["score"] * relevance, 2)

        # Actualizar en Supabase
        update_data = {
            "semantic_score": relevance,
            "score": final_score,
            "semantic_reason": result["reason"]
        }

        if relevance < min_relevance:
            update_data["status"] = "rejected"
            rejected += 1
        else:
            approved += 1

        supabase.table("posts").update(update_data).eq("id", post["id"]).execute()

    print(f"\n📊 Aprobados: {approved} | Rechazados: {rejected}")


if __name__ == "__main__":
    run()