# Multi Tenancy Spring Boot 4

Minimal Spring Boot 4 university example for Axoniq multi-tenancy.

## What it shows

- one `tenantId` per request path
- commands and queries routed with `tenantId` metadata
- tenant-specific in-memory read models via `TenantComponentRegistry`

## Run

Start Axon Server locally, then run the application from this module.

## Example requests

```bash
curl -X POST http://localhost:8080/tenants/foo-a/courses \
  -H 'Content-Type: application/json' \
  -d '{"courseId":"demo-1","name":"Event Sourcing in Practice","capacity":30}'

curl http://localhost:8080/tenants/foo-a/courses
curl http://localhost:8080/tenants/foo-a/courses/demo-1
```
