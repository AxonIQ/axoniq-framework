# Verify event processor support

## Issue Type

- [ ] Feature
- [ ] Enhancement
- [ ] Bug
- [x] Documentation

## Summary

Verify the event processor support items already listed as complete in the multi-tenancy todo.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: marked as completed in the todo list
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- `MultiTenantEventProcessor`
- `MultiTenantEventProcessorModule`
- `MultiTenantPooledStreamingEventProcessorModule`

## What We Need

- Confirm the event processor modules are present
- Confirm they work with the current multi-tenancy wiring
- Confirm the docs and tests still reflect the current behavior

## Gap

- Current behaviour: the todo says event processor support is already available
- Wanted behaviour: the implementation is verified against the current source tree
- Possible workarounds: none

## Scope

- In scope:
  - module verification
  - wiring review
  - docs/example alignment
- Out of scope:
  - new processor variants
  - reworking the processor model

## Non-Goals

- Adding new event processor features
- Revisiting the processor architecture

## Notes

- Treat this as a confirmation pass for already completed support

## Acceptance Criteria

- [ ] The processor modules are still available
- [ ] The multi-tenant wiring still connects correctly
- [ ] The docs or examples mention the current support path

## Verification

- Inspect the processor modules and their tests
- Cross-check any example wiring against the module code

