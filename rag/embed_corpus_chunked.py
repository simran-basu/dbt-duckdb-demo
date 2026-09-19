import os
import chromadb
from sentence_transformers import SentenceTransformer
from langchain_text_splitters import RecursiveCharacterTextSplitter

model = SentenceTransformer("all-MiniLM-L6-v2")

client = chromadb.PersistentClient(path="./chroma_db")
# Fresh collection for the chunked version, so you can compare against Tuesday's naive one
collection = client.get_or_create_collection("pharma_knowledge_chunked")

# --- Chunking config ---
# chunk_size in characters (roughly 4 chars/token, so ~300 tokens ≈ 1200 chars)
# chunk_overlap ~15% of chunk_size
splitter = RecursiveCharacterTextSplitter(
    chunk_size=400,
    chunk_overlap=60,
    separators=["\n\n", "\n", ". ", " ", ""],  # tries paragraph, then line, then sentence, then word
)

corpus_dir = "./corpus"
documents = []
metadatas = []
ids = []
chunk_counter = 0

for filename in sorted(os.listdir(corpus_dir)):
    filepath = os.path.join(corpus_dir, filename)
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read()

    chunks = splitter.split_text(text)

    for chunk_index, chunk in enumerate(chunks):
        documents.append(chunk)
        metadatas.append({
            "source": filename,
            "chunk_index": chunk_index,
            "total_chunks_in_doc": len(chunks),
        })
        ids.append(f"chunk_{chunk_counter}")
        chunk_counter += 1

print(f"Split {len(os.listdir(corpus_dir))} documents into {len(documents)} chunks")

embeddings = model.encode(documents).tolist()

collection.add(
    documents=documents,
    embeddings=embeddings,
    metadatas=metadatas,
    ids=ids,
)

print(f"Stored {collection.count()} chunk records in 'pharma_knowledge_chunked'")

# --- Inspect a few chunks to see the effect ---
print("\n=== Sample chunks ===")
for i in range(min(3, len(documents))):
    print(f"\nChunk {i} | source: {metadatas[i]['source']} | chunk {metadatas[i]['chunk_index']+1}/{metadatas[i]['total_chunks_in_doc']}")
    print(documents[i])

# --- Test retrieval with chunked data ---
query = "What is the dosing for cancer treatment?"
query_embedding = model.encode([query]).tolist()

results = collection.query(query_embeddings=query_embedding, n_results=3)

print("\n=== Query results (chunked) ===")
for doc, meta, distance in zip(results["documents"][0], results["metadatas"][0], results["distances"][0]):
    print(f"Source: {meta['source']} (chunk {meta['chunk_index']+1}/{meta['total_chunks_in_doc']}) | Distance: {distance:.4f}")
    print(f"Text: {doc[:150]}...\n")