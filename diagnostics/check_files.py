import os
import json
from pathlib import Path
from src.config import COUNTER_FILE, METADATA_FILE, CHECKPOINT_FILE

print("=" * 60)
print("CHECKING FILES")
print("=" * 60)

if Path(COUNTER_FILE).exists():
    with open(COUNTER_FILE, 'r') as f:
        print(f"Counter: {f.read().strip()}")
else:
    print("Counter: NOT FOUND")

if Path(METADATA_FILE).exists():
    with open(METADATA_FILE, 'r') as f:
        meta = json.load(f)
        print(f"Metadata total_records: {meta.get('total_records', 'NOT SET')}")
else:
    print("Metadata: NOT FOUND")

if Path(CHECKPOINT_FILE).exists():
    with open(CHECKPOINT_FILE, 'r') as f:
        ckpt = json.load(f)
        print(f"Checkpoint state: {ckpt.get('state', 'unknown')}")
        print(f"Checkpoint timestamp: {ckpt.get('timestamp', 'NOT SET')}")
        print(f"Checkpoint count: {ckpt.get('count', 'NOT SET')}")
else:
    print("Checkpoint: NOT FOUND")

print("\n" + "=" * 60)
