import json

with open('data/transaction_log.json', 'r') as f:
    log = json.load(f)

print(f"Total transactions: {len(log)}")
print("\nTx States:")
for tx in log:
    print(f"  ID: {tx['transaction_id'][:8]}... | State: {tx['state']} | Op: {tx['operation']}")

states = {}
for entry in log:
    state = entry.get("state")
    states[state] = states.get(state, 0) + 1

print(f"\nState summary: {states}")
committed = states.get("committed", 0)
success_rate = (committed / len(log) * 100) if len(log) > 0 else 0
print(f"Success rate: {committed}/{len(log)} = {success_rate:.1f}%")
