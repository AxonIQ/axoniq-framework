# Verify example coverage for `examples/multi-tenancy-jdbc-java`

## Issue Type

- [ ] Feature
- [ ] Enhancement
- [ ] Bug
- [x] Documentation

## Summary

Verify the example coverage listed in the todo for `examples/multi-tenancy-jdbc-java`.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: marked as completed in the todo list
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- tenant-specific H2 datasources
- tenant-specific projections
- tenant-specific JDBC token store wiring
- tenant-specific transaction manager wiring

## What We Need

- Confirm the example still demonstrates the listed tenant-specific wiring
- Confirm the example matches the current module APIs
- Confirm the docs can point to the example as a working reference

## Gap

- Current behaviour: the todo says the example coverage already exists
- Wanted behaviour: the example is verified against the current codebase
- Possible workarounds: none

## Scope

- In scope:
  - example verification
  - wiring review
  - docs cross-reference review
- Out of scope:
  - adding new example scenarios
  - changing example architecture

## Non-Goals

- Rebuilding the example from scratch
- Adding unrelated example modules

## Notes

- This issue is mostly a safety net to confirm the example remains aligned

## Acceptance Criteria

- [ ] The example still contains the listed tenant-specific wiring
- [ ] The example builds against the current module API
- [ ] The docs can link to it as a working reference

## Verification

- Inspect the example module contents and build status
- Confirm the example reflects the current tenant-local wiring choices

