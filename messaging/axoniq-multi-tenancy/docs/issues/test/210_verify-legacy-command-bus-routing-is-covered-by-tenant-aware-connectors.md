# Verify legacy command bus routing is covered by tenant-aware connectors

## Issue Type

- [ ] Feature
- [ ] Enhancement
- [ ] Bug
- [x] Documentation

## Summary

Verify that the legacy full-surface command bus routing is intentionally covered by the tenant-aware connector layer instead of a separate old-style command bus implementation.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: marked as solved differently in the todo list
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- Tenant-aware bus connector layer
- Command routing support in the current multi-tenancy module

## What We Need

- Confirm the connector layer is the supported path
- Confirm no separate legacy command bus surface is needed

## Gap

- Current behaviour: the todo says the old full surface is not the chosen solution
- Wanted behaviour: the supported path is verified and documented
- Possible workarounds: none

## Scope

- In scope:
  - connector path verification
  - docs alignment
  - scope confirmation
- Out of scope:
  - rebuilding the old command bus layer
  - adding legacy command interception helpers

## Non-Goals

- Reintroducing `TenantRoutingCommandBus`
- Reintroducing `TenantCommandSegmentFactory`

## Notes

- The issue exists to make the supported command routing path explicit

## Acceptance Criteria

- [ ] The connector path is confirmed as the supported solution
- [ ] No separate legacy command bus surface is required
- [ ] The docs reflect the current routing choice

## Verification

- Inspect the current command routing classes and tests
- Confirm the old surface is intentionally not part of the port

