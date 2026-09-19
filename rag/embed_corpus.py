import os
import chromadb
from sentence_transformers import SentenceTransformer

# --- Load a small, fast local embedding model ---
# all-MiniLM-L6-v2 is a common lightweight choice: 384-dim vectors, runs fine on CPU
model = SentenceTransformer("all-MiniLM-L6-v2")

# --- Set up Chroma's local persistent client ---
client = chromadb.PersistentClient(path="./chroma_db")
collection = client.get_or_create_collection("pharma_knowledge")

# --- Read all corpus files ---
corpus_dir = "./corpus"
documents = []
metadatas = []
ids = []

for i, filename in enumerate(sorted(os.listdir(corpus_dir))):
    filepath = os.path.join(corpus_dir, filename)
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read()

    documents.append(text)
    metadatas.append({"source": filename})
    ids.append(f"doc_{i}")

print(f"Loaded {len(documents)} documents from corpus/")

# --- Generate embeddings ---
embeddings = model.encode(documents).tolist()

print(f"Generated {len(embeddings)} embeddings, each of length {len(embeddings[0])}")

# --- Store in Chroma ---
collection.add(
    documents=documents,
    embeddings=embeddings,
    metadatas=metadatas,
    ids=ids,
)

print(f"Stored {collection.count()} records in Chroma collection 'pharma_knowledge'")

# --- Inspect one record to understand the structure ---
result = collection.get(ids=["doc_0"], include=["documents", "metadatas", "embeddings"])
print("\n=== Example record ===")
print("ID:", result["ids"][0])
print("Metadata:", result["metadatas"][0])
print("Document (first 200 chars):", result["documents"][0][:200])
print("Embedding (first 10 values):", result["embeddings"][0][:10])
print("Embedding length:", len(result["embeddings"][0]))

query = "What cancer drugs are available?"
query_embedding = model.encode([query]).tolist()

results = collection.query(
    query_embeddings=query_embedding,
    n_results=2,
)

print("\n=== Query results ===")
for doc, meta, distance in zip(results["documents"][0], results["metadatas"][0], results["distances"][0]):
    print(f"Source: {meta['source']} | Distance: {distance:.4f}")
    print(f"Text: {doc[:150]}...\n")