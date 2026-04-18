# CRUD Query Examples

This page shows the basic request shapes supported by the logical query interface.

Use these JSON bodies in the dashboard query form. The dashboard is the expected way to test CRUD from localhost.

## 1. Create

Create inserts a new logical record.

```json
{
  "operation": "CREATE",
  "entity": "main_records",
  "payload": {
    "username": "alice01",
    "name": "Alice Johnson",
    "age": 29,
    "city": "Seattle",
    "subscription": "premium"
  }
}
```

Paste this into the dashboard query form when testing CREATE.

## 2. Read

Read returns records that match filters. You can also select specific columns.

```json
{
  "operation": "READ",
  "entity": "main_records",
  "filters": {
    "city": "Seattle",
    "subscription": "premium"
  },
  "columns": ["username", "city", "subscription"]
}
```

Paste this into the dashboard query form when testing READ.

## 3. Update

Update changes records that match the filter conditions.

```json
{
  "operation": "UPDATE",
  "entity": "main_records",
  "filters": {
    "username": "alice01"
  },
  "payload": {
    "subscription": "trial",
    "city": "Redmond"
  }
}
```

Paste this into the dashboard query form when testing UPDATE.

## 4. Delete

Delete removes records that match the filter conditions.

```json
{
  "operation": "DELETE",
  "entity": "main_records",
  "filters": {
    "username": "alice01"
  }
}
```

Paste this into the dashboard query form when testing DELETE.

## Notes

- `CREATE` requires a non-empty `payload`.
- `READ` requires `filters` and can include optional `columns`.
- `UPDATE` requires both `filters` and `payload`.
- `DELETE` requires `filters`.
- Replace `main_records` with the logical entity name used by your schema.
