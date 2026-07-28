# Verify tenant projection transactions for JPA and JDBC

## Issue Type

- [ ] Feature
- [ ] Enhancement
- [ ] Bug
- [x] Documentation

## Summary

Verify tenant-specific projection transactions for both JPA and JDBC.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: listed as a likely next technical gap
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- JDBC tenant transaction bridging
- Projection support in the example module

## What We Need

- JPA projection transaction coverage
- JDBC projection transaction coverage
- Evidence that both paths work with tenant-local resources

## Gap

- Current behaviour: the todo calls out missing coverage for tenant projection transactions
- Wanted behaviour: both JDBC and JPA projection transaction paths are covered
- Possible workarounds: manual integration testing

## Scope

- In scope:
  - JPA transaction verification
  - JDBC transaction verification
  - example or integration test coverage
- Out of scope:
  - new transaction abstractions
  - unrelated persistence behavior

## Non-Goals

- Expanding the projection model
- Reworking transaction semantics

## Notes

- Keep this as a behavior verification issue, not a design discussion

## Acceptance Criteria

- [ ] JDBC projection transactions are covered
- [ ] JPA projection transactions are covered
- [ ] The coverage proves tenant-local behavior

## Verification

- Add or run focused projection transaction tests for both stacks
- Confirm the tenant-specific resources are used in each path

