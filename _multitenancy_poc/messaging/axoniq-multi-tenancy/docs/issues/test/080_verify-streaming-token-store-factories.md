# Verify streaming token store factories

## Issue Type

- [ ] Feature
- [ ] Enhancement
- [ ] Bug
- [x] Documentation

## Summary

Verify the token store factory support that is already checked off in the multi-tenancy todo.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: marked as completed in the todo list
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- In-memory token store factory support
- JDBC token store factory support
- JPA token store factory support

## What We Need

- Confirm the factories exist in the current source tree
- Confirm they are wired for tenant-local usage
- Confirm the docs or examples still point at the right factory choices

## Gap

- Current behaviour: the todo lists these factories as already available
- Wanted behaviour: the factory support is verified and documented
- Possible workarounds: manual store construction

## Scope

- In scope:
  - factory verification
  - tenant-local wiring review
  - docs review
- Out of scope:
  - new store backends
  - changing token store semantics

## Non-Goals

- Reworking the token store abstraction
- Adding extra factory variants

## Notes

- Keep this focused on confirming the supported options, not expanding them

## Acceptance Criteria

- [ ] The in-memory, JDBC, and JPA factories are present
- [ ] The current docs reflect those options
- [ ] The tenant-local wiring is still valid

## Verification

- Inspect the current factory implementations
- Check any examples or tests that exercise tenant-local token stores

