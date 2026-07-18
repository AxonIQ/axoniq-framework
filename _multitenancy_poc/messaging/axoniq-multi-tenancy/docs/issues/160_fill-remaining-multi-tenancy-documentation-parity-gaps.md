# Fill remaining multi-tenancy documentation parity gaps

## Issue Type

- [ ] Feature
- [ ] Enhancement
- [ ] Bug
- [x] Documentation

## Summary

Fill the remaining documentation parity gaps between the legacy `_old` multi-tenancy docs and the current Axon Framework reference guide.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: the legacy documentation exists, but the current reference guide is still incomplete
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- Legacy reference material in `_old/docs`
- A current reference-guide structure in `docs/reference-guide`
- Existing multi-tenancy module notes

## What We Need

- Reference docs for configuration
- Disable/enable guides
- Multi-tenant component catalog

## Gap

- Current behaviour: the reference guide does not yet cover the full multi-tenancy doc set
- Wanted behaviour: the current reference guide reflects the supported multi-tenancy story
- Possible workarounds: reading the legacy docs

## Scope

- In scope:
  - documentation migration
  - terminology cleanup
  - xref and example updates
- Out of scope:
  - runtime feature changes
  - preserving the legacy doc structure

## Non-Goals

- Copying `_old` wording verbatim
- Documenting unsupported legacy internals as if they are current

## Notes

- Treat the current docs as the source of truth once the migration lands

## Acceptance Criteria

- [ ] The reference guide covers the missing multi-tenancy topics
- [ ] The docs match current module names and behavior
- [ ] Legacy-only references are removed or replaced

## Verification

- Compare the reference guide against the remaining `_old` material
- Confirm the migrated pages read as current status, not as migration notes

