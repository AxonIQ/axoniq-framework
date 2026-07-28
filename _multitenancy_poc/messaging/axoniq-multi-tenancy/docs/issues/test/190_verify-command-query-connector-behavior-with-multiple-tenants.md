# Verify command and query connector behavior with multiple tenants

## Issue Type

- [ ] Feature
- [ ] Enhancement
- [ ] Bug
- [x] Documentation

## Summary

Verify the command and query connector behavior when multiple tenants are active at the same time.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: listed as a likely next technical gap
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- Multi-tenant command/query routing connectors
- Existing tenant-aware event store routing

## What We Need

- Coverage for connector behavior with multiple active tenants
- Confirmation that routing stays isolated per tenant

## Gap

- Current behaviour: the todo calls out missing multi-tenant connector coverage
- Wanted behaviour: the connector path is verified with more than one active tenant
- Possible workarounds: single-tenant checks only

## Scope

- In scope:
  - multi-tenant connector verification
  - isolation checks
  - test coverage
- Out of scope:
  - new connector types
  - command/query routing redesign

## Non-Goals

- Rebuilding the routing layer
- Introducing extra registry layers

## Notes

- Focus on interaction between active tenants, not just one tenant at a time

## Acceptance Criteria

- [ ] The command connector behaves correctly with multiple tenants
- [ ] The query connector behaves correctly with multiple tenants
- [ ] The tests show tenant isolation

## Verification

- Add a test that activates multiple tenants and exercises both connectors
- Confirm there is no cross-tenant routing leakage

