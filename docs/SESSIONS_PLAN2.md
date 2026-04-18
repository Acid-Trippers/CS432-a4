# Sessions Plan

So what I want is whenever I start `localhost:8080`, it gets on a welcome page and gives two options: Whether I am an admin or a user. The welcome page should also tell whether is initialized yet or not. If it's not initialized, the user button should be locked because there's nothing user will be able to do. A message should pop up: "System is not initialised yet. Admin should initialize the system first". The client must be admin to proceed.

Then client presses the admin option, enters password and depending on what state the pipeline (initialized/fresh/schema defined) the next steps continue as usual. Once pipeline is initialized, admin is redirected to the admin dashboard (`localhost:8080/dashboard/admin`) which should look exactly how the current dashboard looks. In other words, move current `localhost:8080/dashboard` contents to `localhost:8080/dashboard/admin`.

If pipeline is initialized already, then client can also press the user button. Then he is shown a dialog box where he is given the option to create a new session or continue from an existing session. Next, he goes to user dashboard (`localhost:8080/dashboard/{session-uuid}`).

Here are exact details of what both dashboards should contain.

### Normal User
1. CRUD transactions (autocommit/multistep both)
2. Fetch
3. Basic stats
4. Entry point dialog box: Option to create a new session or continue from an existing session. 
5. When they create a new session, they are asked to give a title to a session. This title will be shown in the list of active session when a normal user is trying to open an existing session.
6. Logical Data Browser (not implemented yet, not part of this plan)

### Admin
1. Initialise
2. Reset everything
3. Performance benchmarks
4. ACID tests
5. Basic + Developer mode stats
6. Anything else in developer mode
7. View all sessions

## More Details
1. The admin password should be a simple hardcoded string in config.py (e.g., `ADMIN_PASS = "admin123"`). Do not build a complex database-backed user authentication system.
2. Configure FastAPI to serve the user dashboard template at @app.get("/dashboard/{session_id}"). The frontend JS must extract this session_id from the URL and use it in the X-Session-ID header for all subsequent API calls.
3. Do not alter the core execution logic in `transaction_coordinator.py` or the underlying `mongo_engine.py`/`sql_engine.py`. This task is strictly a frontend routing, UI rendering, and FastAPI endpoint-wrapping exercise to implement the Session Manager and Admin/User views.