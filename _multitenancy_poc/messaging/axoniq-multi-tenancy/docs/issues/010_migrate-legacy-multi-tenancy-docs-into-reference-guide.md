# Migrate legacy multi-tenancy docs into the reference guide

## Issue Type

- [ ] Feature
- [ ] Enhancement
- [ ] Bug
- [x] Documentation

## Summary

Migrate the existing multi-tenancy documentation from the legacy `_old/docs` tree into the Axoniq Framework reference guide under `docs/reference-guide`, so the feature is documented in the active docs set instead of only living in the historical source tree.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: the feature work and reference material live in the module, but the published reference guide does not yet contain the migrated docs
- Origin / reference: legacy documentation under `_old/docs`

## What We Have

- Legacy multi-tenancy documentation in `_old/docs`
- Current module-local notes that compare `_old` and `_tmp` behavior
- A reference guide structure under `docs/reference-guide`

## What We Need

- Migrate the useful legacy documentation into `docs/reference-guide`
- Present the content as Axoniq Framework status quo, not as a historical port note
- Keep the docs aligned with the current multi-tenancy API and terminology
- Add or update any xrefs, examples, and supporting pages needed for the reference guide

## Gap

- Current behaviour: the old multi-tenancy documentation is still only available under `_old/docs`
- Wanted behaviour: the multi-tenancy documentation is integrated into `docs/reference-guide` as the active Axoniq Framework documentation
- Possible workarounds: none

## Scope

- In scope:
  - move relevant content from `_old/docs`
  - rewrite it for the current reference guide structure
  - update terminology and examples where needed
- Out of scope:
  - redesigning multi-tenancy behavior
  - implementing new runtime features
  - preserving `_old` wording verbatim

## Non-Goals

- Reworking the feature itself
- Adding new multi-tenancy capabilities beyond documentation migration
- Keeping the legacy doc structure intact as the user-facing source

## Notes

- Treat this as a reminder issue so the documentation migration is not missed during feature work
- Prefer documenting the current API and configuration as the default user path
- If legacy content no longer fits the current reference guide, replace it with the current Axon 5 behavior rather than carrying over outdated framing

## Acceptance Criteria

- [ ] Relevant multi-tenancy documentation exists in `docs/reference-guide`
- [ ] The migrated pages describe the current Axoniq Framework behavior and terminology
- [ ] Any references to the old doc tree are removed or replaced with reference-guide links

## Verification

- Confirm the relevant multi-tenancy topics are reachable from `docs/reference-guide`
- Check that the migrated pages no longer depend on `_old/docs` as the primary source
- Review xrefs and examples for consistency with the current module structure

