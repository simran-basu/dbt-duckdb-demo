from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from retrieve import retrieve_with_threshold

tokenizer = AutoTokenizer.from_pretrained("google/flan-t5-base")
model = AutoModelForSeq2SeqLM.from_pretrained("google/flan-t5-base")


def build_prompt(query: str, chunks: list) -> str:
    if not chunks:
        return None

    context = "\n\n".join(f"[{c['source']}]: {c['text']}" for c in chunks)

    prompt = f"""Answer the question using ONLY the context below. If the context does not contain enough information to answer, say "I don't have enough information to answer this."

Context:
{context}

Question: {query}

Answer:"""
    return prompt


def generate_answer(prompt: str, max_new_tokens: int = 150) -> str:
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=512)
    outputs = model.generate(**inputs, max_new_tokens=max_new_tokens)
    return tokenizer.decode(outputs[0], skip_special_tokens=True)


def rag_answer(query: str, top_k: int = 3, max_distance: float = 1.3):
    chunks = retrieve_with_threshold(query, top_k=top_k, max_distance=max_distance)
    prompt = build_prompt(query, chunks)

    if prompt is None:
        return {
            "query": query,
            "answer": "I don't have enough information to answer this.",
            "sources": [],
        }

    answer = generate_answer(prompt)

    return {
        "query": query,
        "answer": answer,
        "sources": [c["source"] for c in chunks],
    }


if __name__ == "__main__":
    test_queries = [
        "What is the mechanism of action for the cancer drug?",
        "What are the side effects of the heart failure medication?",
        "How does climate change affect crop yields?",
    ]

    for q in test_queries:
        result = rag_answer(q)
        print(f"\n{'='*70}")
        print(f"QUERY: {result['query']}")
        print(f"ANSWER: {result['answer']}")
        print(f"SOURCES: {result['sources']}")