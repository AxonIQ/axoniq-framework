# Define the legacy `_old` features that stay out of scope

## Issue Type

- [ ] Feature
- [ ] Enhancement
- [ ] Bug
- [x] Documentation

## Summary

Define which legacy `_old` multi-tenancy features are intentionally out of scope for the Axoniq Framework port.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: the todo notes that some legacy features may not be carried forward
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- Legacy `_old` implementation trees for reference
- Current module decisions that solve some legacy problems differently

## What We Need

- A clear list of features that should not be ported
- A short rationale for each excluded item
- Documentation that keeps the scope visible for future work

## Gap

- Current behaviour: the out-of-scope boundary is only partially captured in notes
- Wanted behaviour: the project has a clear written scope for the port
- Possible workarounds: reading the legacy trees directly

## Scope

- In scope:
  - scope decisions
  - short rationale notes
  - documentation of exclusions
- Out of scope:
  - implementing the excluded features
  - reviving the old legacy surface

## Non-Goals

- Reintroducing legacy APIs by accident
- Turning this into a design rewrite

## Notes

- This issue is about making the port boundary explicit

## Acceptance Criteria

- [ ] The excluded legacy features are listed clearly
- [ ] Each exclusion has a short reason
- [ ] The scope is documented where the multi-tenancy work is tracked

## Verification

- Review the legacy `_old` tree against the current module plan
- Confirm the documented scope matches the current implementation direction

