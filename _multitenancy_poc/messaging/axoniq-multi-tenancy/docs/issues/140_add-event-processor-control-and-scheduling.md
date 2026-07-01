# Add event processor control and scheduling

## Issue Type

- [x] Feature
- [ ] Enhancement
- [ ] Bug
- [ ] Documentation

## Summary

Add the missing event processor control and scheduling capabilities that exist in the legacy multi-tenancy trees.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: present in `_old`, but not yet covered in the current module
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- Existing multi-tenant event processor support
- Framework-level processor infrastructure

## What We Need

- Scheduler support
- Event processor control segments
- Dead-letter queue support

## Gap

- Current behaviour: the control and scheduling pieces are missing from the current module
- Wanted behaviour: multi-tenant event processors can be scheduled and controlled like the legacy feature set
- Possible workarounds: manual scheduling or external control logic

## Scope

- In scope:
  - scheduler integration
  - processor control segments
  - DLQ integration
- Out of scope:
  - full event processor redesign
  - unrelated scheduling frameworks

## Non-Goals

- Introducing a new scheduling abstraction
- Expanding beyond multi-tenant processor control

## Notes

- Keep the control layer thin and aligned with the current framework APIs

## Acceptance Criteria

- [ ] Event processors can be controlled per tenant
- [ ] Scheduling support exists for the multi-tenant path
- [ ] DLQ behavior is available where required

## Verification

- Add tests for the control and scheduling path
- Confirm the tenant-specific behavior works without legacy routing helpers

