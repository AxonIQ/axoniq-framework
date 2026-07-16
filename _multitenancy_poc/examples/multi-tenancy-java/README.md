# Multi Tenancy Java

## Monitoring

* [Axon Server (localhost)](http://localhost:8024/)
* [Axon Platform](https://platform.dev.axoniq.net/)


## Current state - 17.06.2026

- The example module wires `CourseStatsRepository` as a tenant-scoped component instead of a singleton.
- `CoursesStatsProjection` and `GetCourseStatsByIdQueryHandler` resolve the repository from the current message tenant via method parameter injection.
- The repl includes a `get-course-stats-by-id` command that accepts `tenant` and `id`, so the read model can be queried tenant-by-tenant from the console.
- The example keeps the strict `context-name = tenantId` rule, which matches the Axon Server tenant context to the tenant descriptor used by the multi-tenancy layer.
- For the event stream, one persistent stream per tenant is still the right model: the tenant-specific context is what keeps the projection segments isolated.
