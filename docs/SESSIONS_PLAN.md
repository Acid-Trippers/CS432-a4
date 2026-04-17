# Connection-based Strictly Isolated Sessions - PLAN

### Phase 1: Persistent Session State Management (Backend)
**The Approach:** Instead of relying entirely on volatile RAM, you will create a `sessions.json` file to act as the definitive registry of all client interactions. 
* **Structure:** A JSON dictionary where the key is a unique `Session_ID` (UUID) and the value is an object containing: `status` (active/idle), `started_at`, `last_active`, `transaction_history` (list of IDs and outcomes), and `query_history`.
* **Query Format:** The `query_history` array will store the exact JSON payload the user submits (e.g., `{ operation: "READ", entity: "main_records"... }`), rather than translating it to SQL strings.
* **Reasoning:** A file-backed registry guarantees that if the FastAPI server restarts, you do not lose the history of who was doing what. It provides a simple, queryable data source for the dashboard's session monitor.

### Phase 2: Global Concurrency Control (Strict ACID Enforcer)
**The Approach:**
You will introduce a global `in_transaction` mutex (mutual exclusion) flag at the application level, likely inside your transaction coordinator or backend state manager.
* **Workflow:** 1. When a client submits a multi-query transaction (via `BEGIN`), the system checks the `in_transaction` flag.
  2. If `True`, the backend immediately rejects the request with a "System busy" error, forcing the client to wait. 
  3. If `False`, the system sets it to `True`, locks out all other clients, and begins executing the queries via your Saga pattern.
  4. Upon completion (either a successful commit or a compensatory rollback), the flag is flipped back to `False`.
* **Reasoning:** This is the easiest and most foolproof way to guarantee strict Isolation. It mathematically eliminates the possibility of dirty reads or write-conflicts because only one transaction can exist in the execution pipeline at any given millisecond.

### Phase 3: Identity Injection via FastAPI
**The Approach:**
You need a mechanism to securely link incoming HTTP requests to the `sessions.json` registry without building a complex user authentication system.
* **Workflow:** Implement a FastAPI dependency (e.g., `get_session_id`) that intercepts every request to your `/query` and `/pipeline` routers. It will look for a custom HTTP header like `X-Session-ID`. 
* **Initialization:** If the header is missing, the backend generates a new UUID, creates a fresh entry in `sessions.json`, and returns the ID to the client.
* **Reasoning:** This creates a seamless "Connection-Based Session." The middleware handles client identification invisibly, ensuring every CRUD operation naturally carries the context of *who* is executing it.

### Phase 4: Linking Execution to the Session
**The Approach:**
Your existing `query_runner` and `transaction_coordinator` must be updated to accept the injected `Session_ID` and log their activities.
* **Workflow:** * Every time an auto-commit query executes successfully, its JSON payload is appended to the `query_history` of that specific session in `sessions.json`. 
  * Every time a manual transaction concludes, its outcome (e.g., `{"tx_id": "tx-101", "status": "COMMITTED"}`) is appended to `transaction_history`.
  * The `last_active` timestamp is updated upon any activity.
  * *Crucial check:* A background worker or startup script should periodically sweep `sessions.json` and change the status of sessions inactive for more than 30 minutes from "active" to "archived."
* **Reasoning:** This establishes the crucial "ownership" link. The backend operations remain the same, but their execution footprint is neatly categorized by the client who initiated them.

### Phase 5: Dashboard Identity & Monitoring (Frontend)
**The Approach:**
The frontend must be upgraded to hold its identity and provide the "Active Sessions" visualization required by the assignment.
* **Identity Persistence:** In `main.js`, you will use browser `localStorage` to save the `Session_ID`. Every `fetch()` call made by the dashboard will explicitly attach this ID in the headers. 
* **The Session Monitor View:** You will build a new UI panel on the dashboard. This panel will poll a new backend endpoint (`GET /api/stats/sessions`) every few seconds. 
* **Visualization:** It will display a table of all entries in `sessions.json`. It will show which UUIDs are "Active", how long they have been connected, and a collapsible view of their JSON `query_history`. You will visually highlight the row that matches the user's current `localStorage` ID so they know which session is theirs.
* **Reasoning:** This fulfills the exact parameters of the "Viewing Active Sessions" requirement. By separating the Session Monitor from the data querying interface, you set a clean architectural foundation. 

Once this pipeline is complete, the middleware will successfully track distinct clients, prevent them from overlapping during transactions, and display their activity live. This sets the stage perfectly to implement the Logical Data Browser, as the system will already know exactly which client is asking to view the schema.