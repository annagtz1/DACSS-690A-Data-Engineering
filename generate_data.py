"""
generate_data.py
Creates data/orders_large.csv by repeating a tiny sample until the file size >= 100MB.
Run: python generate_data.py
"""
import pandas as pd
from pathlib import Path

OUT = Path("data")
OUT.mkdir(exist_ok=True)
sample_path = OUT / "orders_sample.csv"
large_path = OUT / "orders_large.csv"

if not sample_path.exists():
    df = pd.DataFrame({
        "order_id": [f"o{i}" for i in range(1, 11)],
        "order_purchase_timestamp": ["2020-01-01 10:00:00"]*10,
        "order_approved_at": ["2020-01-01 10:05:00"]*10,
        "order_delivered_carrier_date": ["2020-01-05 10:00:00"]*10,
        "price": [10.0, 20.0, 30.0, 5.0, 7.5, 12.0, 6.0, 8.0, 15.0, 3.0]
    })
    df.to_csv(sample_path, index=False)
else:
    df = pd.read_csv(sample_path)

target_bytes = 100 * 1024 * 1024
chunks = []
current_bytes = 0
rep = 0
while current_bytes < target_bytes:
    chunks.append(df.assign(rep=rep))
    rep += 1
    current_bytes = sum(len(chunk.to_csv(index=False).encode("utf-8")) for chunk in chunks)

big_df = pd.concat(chunks, ignore_index=True)
big_df.to_csv(large_path, index=False)
print(f"Wrote {large_path} size: {large_path.stat().st_size / (1024*1024):.2f} MB")
