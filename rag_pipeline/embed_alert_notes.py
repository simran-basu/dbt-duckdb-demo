import pandas as pd
import chromadb
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")
client = chromadb.PersistentClient(path="./chroma_db")
collection = client.get_or_create_collection("customer_alert_notes")

df = pd.read_csv("alert_notes.csv")

documents = df["alert_note"].tolist()
metadatas = [
    {
        "customer_id": int(row["customer_id"]),
        "customer_name": row["customer_name"],
        "region": row["region"],
        "alert_reason": row["alert_reason"],
        "target_tier": row["target_tier"],
    }
    for _, row in df.iterrows()
]
ids = [f"alert_note_{cid}" for cid in df["customer_id"]]

embeddings = model.encode(documents).tolist()

collection.add(
    documents=documents,
    embeddings=embeddings,
    metadatas=metadatas,
    ids=ids,
)

print(f"Stored {collection.count()} alert notes in 'customer_alert_notes' collection")