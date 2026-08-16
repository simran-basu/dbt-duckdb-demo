import chromadb
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")
client = chromadb.PersistentClient(path="./chroma_db")
collection = client.get_collection("pharma_knowledge_chunked")


def retrieve(query: str, top_k: int = 3, source_filter: str = None):
    """
    Embeds a question and returns the top-k most similar chunks from Chroma.

    Args:
        query: the natural-language question
        top_k: how many chunks to return
        source_filter: optional filename to restrict search to one document
    """
    query_embedding = model.encode([query]).tolist()

    where_clause = {"source": source_filter} if source_filter else None

    results = collection.query(
        query_embeddings=query_embedding,
        n_results=top_k,
        where=where_clause,
    )

    retrieved = []
    for doc, meta, distance in zip(
        results["documents"][0], results["metadatas"][0], results["distances"][0]
    ):
        retrieved.append({
            "text": doc,
            "source": meta["source"],
            "chunk": f"{meta['chunk_index']+1}/{meta['total_chunks_in_doc']}",
            "distance": round(distance, 4),
        })

    return retrieved


def print_results(query: str, results: list):
    print(f"\n{'='*70}")
    print(f"QUERY: {query}")
    print(f"{'='*70}")
    if not results:
        print("No results found.")
        return
    for i, r in enumerate(results, 1):
        print(f"\n[{i}] {r['source']} (chunk {r['chunk']}) | distance: {r['distance']}")
        print(f"    {r['text'][:200]}...")

def retrieve_with_threshold(query: str, top_k: int = 3, max_distance: float = 1.3):
    results = retrieve(query, top_k=top_k)
    filtered = [r for r in results if r["distance"] <= max_distance]
    return filtered

if __name__ == "__main__":
    test_queries = [
        "What are common adherence barriers for rare disease therapies?",
        "What is the mechanism of action for the cancer drug?",
        "What are the side effects of the heart failure medication?",
        "What is the recommended dosing schedule?",
        "How does climate change affect crop yields?",  # deliberately out-of-domain
    ]

    for q in test_queries:
        results = retrieve_with_threshold(q, top_k=3)
        print_results(q, results)