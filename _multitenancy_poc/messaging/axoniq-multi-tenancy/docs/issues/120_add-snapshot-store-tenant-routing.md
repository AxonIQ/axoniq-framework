# Add snapshot store tenant routing

## Issue Type

- [x] Feature
- [ ] Enhancement
- [ ] Bug
- [ ] Documentation

## Summary

Add tenant-aware routing for the snapshot store so snapshot access follows the active tenant.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: present in `_tmp` or `_old`, but not yet covered in the current module
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- Tenant routing for other persistence components
- A multi-tenancy module that already handles tenant-local state in several areas

## What We Need

- `TenantRoutingSnapshotStore`
- `TenantSnapshotStoreSegmentFactory`
- Snapshot access that resolves against the active tenant

## Gap

- Current behaviour: snapshot store tenant routing is not yet covered in the current module
- Wanted behaviour: snapshots route through the tenant-aware store path
- Possible workarounds: manual tenant-specific snapshot store wiring

## Scope

- In scope:
  - snapshot store routing
  - tenant-aware factory support
  - tests or example coverage
- Out of scope:
  - redesigning snapshot semantics
  - unrelated persistence changes

## Non-Goals

- Changing how snapshots are represented
- Introducing a new snapshot concept

## Notes

- This should remain a small routing layer, not a broader persistence rewrite

## Acceptance Criteria

- [ ] Snapshot access resolves by tenant
- [ ] The routing store or factory exists in the module
- [ ] Tests cover tenant-specific snapshot behavior

## Verification

- Add focused tests around tenant-specific snapshot access
- Confirm the factory wiring uses the active tenant

