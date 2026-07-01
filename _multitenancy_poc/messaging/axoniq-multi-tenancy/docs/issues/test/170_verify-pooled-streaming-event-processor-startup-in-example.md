# Verify tenant-specific `PooledStreamingEventProcessor` startup in the example

## Issue Type

- [ ] Feature
- [ ] Enhancement
- [ ] Bug
- [x] Documentation

## Summary

Verify tenant-specific `PooledStreamingEventProcessor` startup in the example module.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: listed as a likely next technical gap
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- Tenant-local projection wiring
- Existing example coverage in `examples/multi-tenancy-jdbc-java`

## What We Need

- Example coverage for tenant-specific pooled streaming processor startup
- Confirmation that startup can resolve the correct tenant-local resources

## Gap

- Current behaviour: the todo calls out missing coverage for this startup path
- Wanted behaviour: the example proves the startup path works for a tenant
- Possible workarounds: manual startup testing

## Scope

- In scope:
  - example coverage
  - startup-path verification
  - tenant resource resolution checks
- Out of scope:
  - new processor features
  - production control-plane behavior

## Non-Goals

- Reworking the example architecture
- Broadening the scope beyond startup verification

## Notes

- Keep the focus on the startup path, not the broader processor lifecycle

## Acceptance Criteria

- [ ] The example starts a tenant-specific pooled streaming processor
- [ ] The startup path resolves tenant-local dependencies
- [ ] The behavior is covered by a test or documented example step

## Verification

- Run the example path and confirm the processor starts for a tenant
- Check for any missing tenant-local resource wiring

