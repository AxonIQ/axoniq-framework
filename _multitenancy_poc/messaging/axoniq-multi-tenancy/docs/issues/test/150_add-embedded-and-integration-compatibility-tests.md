# Add embedded and integration compatibility tests

## Issue Type

- [ ] Feature
- [ ] Enhancement
- [ ] Bug
- [x] Documentation

## Summary

Add or restore the embedded and integration-level compatibility tests that are listed as missing from the current multi-tenancy coverage.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: listed as not yet covered in the todo
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- Some example coverage
- Current module-level tenant support

## What We Need

- Multi-tenant embedded integration tests
- Tenant resolver registry tests beyond the current defaults
- Full factory wiring tests for command, query, and event routing

## Gap

- Current behaviour: compatibility coverage is incomplete
- Wanted behaviour: the current module has embedded and integration tests that cover the main tenant flows
- Possible workarounds: ad hoc manual verification

## Scope

- In scope:
  - embedded integration tests
  - resolver registry tests
  - factory wiring tests
- Out of scope:
  - feature redesign
  - new runtime behavior

## Non-Goals

- Expanding production code solely for tests
- Adding coverage that does not exercise the public API

## Notes

- These tests should verify the existing multi-tenant behavior end-to-end

## Acceptance Criteria

- [ ] Embedded integration coverage exists for the core flows
- [ ] Resolver registry behavior is covered beyond default cases
- [ ] Command/query/event wiring is covered end-to-end

## Verification

- Run the relevant multi-tenant tests against the current module
- Add any missing integration fixtures that exercise the public API

