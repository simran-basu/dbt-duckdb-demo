import chromadb
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")
client = chromadb.PersistentClient(path="./chroma_db")
collection = client.get_collection("customer_alert_notes")


def retrieve_alerts(query: str, top_k: int = 3, region_filter: str = None):
    query_embedding = model.encode([query]).tolist()
    where_clause = {"region": region_filter} if region_filter else None

    results = collection.query(
        query_embeddings=query_embedding,
        n_results=top_k,
        where=where_clause,
    )

    for doc, meta, distance in zip(
        results["documents"][0], results["metadatas"][0], results["distances"][0]
    ):
        print(f"\n[{meta['customer_name']}] tier: {meta['target_tier']} | region: {meta['region']} | distance: {distance:.4f}")
        print(doc)


if __name__ == "__main__":
    print("=== Query: Which customers need retention outreach? ===")
    retrieve_alerts("Which customers need retention outreach due to cancellations?")

    print("\n\n=== Query: Who are our highest value accounts? ===")
    retrieve_alerts("Who are our highest value accounts to prioritize?")

    print("\n\n=== Query filtered to West region only ===")
    retrieve_alerts("customers needing follow-up", region_filter="west")