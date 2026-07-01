# Add processing-context tenancy helpers

## Issue Type

- [x] Feature
- [ ] Enhancement
- [ ] Bug
- [ ] Documentation

## Summary

Add the missing processing-context tenancy helpers so tenant information can be resolved through the current processing context instead of only through direct wiring.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: present in `_tmp` or `_old`, but not yet covered in the current module
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- Tenant resolution APIs
- Existing configuration enhancer hooks
- Processing-context infrastructure elsewhere in the framework

## What We Need

- Tenant-aware processing context wrapper
- Processing-context resolvers and factories
- Annotation-based tenant component resolution

## Gap

- Current behaviour: tenant context helpers are not yet available in the current module
- Wanted behaviour: tenant identity can flow through processing-context helpers
- Possible workarounds: manual tenant propagation in edge code

## Scope

- In scope:
  - context helper implementation
  - resolver/factory support
  - annotation-based resolution hooks
- Out of scope:
  - core processing-context redesign
  - unrelated messaging changes

## Non-Goals

- Replacing the existing processing-context model
- Adding extra tenant-routing layers that duplicate core behavior

## Notes

- Keep the solution aligned with the existing processing-context design

## Acceptance Criteria

- [ ] Tenant helpers exist for processing-context use
- [ ] Resolution flows through the current framework abstractions
- [ ] Tests or examples show the tenant-aware behavior

## Verification

- Add focused tests for context-based tenant resolution
- Confirm the helpers are thin adapters over the current framework model

