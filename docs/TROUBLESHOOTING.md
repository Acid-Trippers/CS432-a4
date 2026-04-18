# Troubleshooting

## 1. Ports already in use

Symptoms:

- docker-compose up fails with bind errors for 5432, 27017, 8000, or 8080.

Fix:

- Stop local services using those ports, or change host port mappings in docker-compose.yml.

## 2. PostgreSQL connection errors

Symptoms:

- Pipeline initialize fails with Postgres connection errors.

Checks:

```bash
docker-compose ps
docker-compose logs postgres --tail 100
```

Fix:

- Ensure POSTGRES_URI points to the correct host/port/credentials.
- If using Docker network from pipeline container, keep host as postgres.

## 3. MongoDB authentication or timeout errors

Symptoms:

- Query or initialization fails with Mongo auth/timeout errors.

Checks:

```bash
docker-compose logs mongo --tail 100
```

Fix:

- Verify MONGO_URI and root credentials in .env and docker-compose values.
- Recreate containers if credentials changed after first boot:

```bash
docker-compose down -v
docker-compose up -d
```

## 4. Ingestion API unreachable

Symptoms:

- Dashboard status reports external API unreachable.

Checks:

```bash
docker-compose logs api --tail 100
curl http://127.0.0.1:8000/
```

Fix:

- Restart api service: docker-compose restart api
- Verify API_HOST environment variable.

## 5. Dashboard starts but setup fails

Symptoms:

- /setup cannot save schema or initialize pipeline.

Fix:

- Confirm initial_schema.json exists.
- Confirm admin login success and valid token cookie.
- Check dashboard logs for /api/pipeline/schema and /api/pipeline/initialise errors.

## 6. Reset to a clean state

Use only if your run is corrupted and you need a clean restart.

```bash
docker-compose down -v
# optional: remove stale files in data/
docker-compose up -d
```

## 7. Verification checklist

- docker-compose ps shows services up.
- http://127.0.0.1:8080 opens.
- /api/pipeline/initialise completes.
- /api/query READ request returns data.
