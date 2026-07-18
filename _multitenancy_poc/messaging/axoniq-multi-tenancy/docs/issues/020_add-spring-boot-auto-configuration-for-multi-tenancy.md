# Add Spring Boot auto-configuration for multi-tenancy

## Issue Type

- [x] Feature
- [ ] Enhancement
- [ ] Bug
- [ ] Documentation

## Summary

Add Spring Boot auto-configuration for the multi-tenancy module so the Spring integration can discover the existing enhancer hooks without requiring manual wiring.

## Context

- Related issue(s): multi-tenancy feature work in this module
- Current release/state: core multi-tenancy support exists, but the Spring Boot autoconfiguration and starter wrapper are still missing
- Origin / reference: `tenant-multi-tenancy-todo.md`

## What We Have

- Core multi-tenancy APIs and enhancer hooks
- Existing Spring integration points that can consume beans
- A separate Spring Boot autoconfiguration/starter module pattern elsewhere in the repository

## What We Need

- Spring Boot autoconfiguration in `dependency-injection/spring/spring-boot-autoconfigure`
- `MultiTenancyConfigurationDefaults` exposed as a Spring bean
- `axon.multitenancy.enabled=false` opt-out support
- Thin `dependency-injection/spring/spring-boot-starter` wrapper after the autoconfiguration exists

## Gap

- Current behaviour: multi-tenancy wiring is available in the core module, but Spring Boot users do not get it automatically
- Wanted behaviour: Spring Boot users get multi-tenancy wiring through autoconfiguration, with an opt-out switch
- Possible workarounds: manual bean wiring

## Scope

- In scope:
  - Spring Boot autoconfiguration module wiring
  - bean exposure for the defaults enhancer
  - starter dependency wrapper
- Out of scope:
  - new multi-tenancy runtime features
  - Spring-specific logic inside the core module

## Non-Goals

- Adding new tenant-routing behavior
- Changing the core multi-tenancy API
- Moving feature logic into Spring packages

## Notes

- Keep the core module free of direct Spring dependencies
- Use the autoconfiguration module to bridge existing core hooks into Spring Boot

## Acceptance Criteria

- [ ] Spring Boot autoconfiguration creates the expected multi-tenancy beans
- [ ] `axon.multitenancy.enabled=false` disables the enhancer wiring
- [ ] A thin starter wrapper exists for consumers

## Verification

- Boot a Spring-based example and confirm the beans are created automatically
- Confirm the opt-out flag prevents the wiring
- Confirm the core module still has no direct Spring dependency

