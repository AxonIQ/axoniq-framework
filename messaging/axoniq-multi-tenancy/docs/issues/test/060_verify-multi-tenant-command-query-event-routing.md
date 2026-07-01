# Verify multi-tenant command, query, and event routing

## Issue Type

- [ ] Feature
- [ ] Enhancement
- [ ] Bug
- [x] Documentation

## Summary

Verify the multi-tenant command, query, and event routing layer that the todo marks as already available for the Axoniq Framework integration.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: marked as completed in the todo list
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- `MultiTenantAxonServerCommandBusConnector`
- `MultiTenantAxonServerQueryBusConnector`
- `TenantRoutingEventStore`

## What We Need

- Confirm the routing layer is still implemented in the current module
- Confirm command, query, and event routing behavior is documented correctly
- Confirm the examples or tests still cover the expected tenant flow

## Gap

- Current behaviour: the todo says this routing layer is already in place
- Wanted behaviour: the implementation and documentation are verified
- Possible workarounds: manual tenant-specific wiring

## Scope

- In scope:
  - routing component verification
  - example and test review
  - docs alignment
- Out of scope:
  - new bus routing mechanisms
  - legacy full-surface command/query bus ports

## Non-Goals

- Reintroducing the old routing surface
- Expanding routing beyond the current connector layer

## Notes

- This should confirm the connector-based solution remains the supported path

## Acceptance Criteria

- [ ] The connector classes exist and are wired correctly
- [ ] The event store routing path is verified
- [ ] The docs or example references match the current design

## Verification

- Inspect the connector code and current tests
- Confirm the supported path is the connector layer rather than legacy buses

