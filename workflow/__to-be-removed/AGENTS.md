# AGENTS.md

## Overview

This repository contains an event-sourced workflow engine built on Axon Framework plus its standard DSL, Spring Boot
integration, examples, tests, and documentation. Changes in one module often affect author-facing DSL APIs, runtime
execution contracts, and example workflows together.

## Folder Structure

- `runtime/`: core workflow execution model, primitives, execution state, configuration, and runtime APIs
- `dsl/`: Java and Kotlin DSL layers built on top of the runtime APIs
- `spring-boot/`: Spring Boot auto-configuration and integration tests
- `examples/simple/`: simple examples of workflows and tests
- `examples/itest/`: in-memory / in-isolation integration-style tests for runtime and DSL behavior
- `examples/bike-rental/`: Spring Boot integration-style tests for the example application
- `examples/`: example aggregator; included in the root reactor by the default-active `examples` profile
- `test/`: shared testing utilities and support code
- `docs/`: ADRs, reference docs, playbook, and getting-started material (see [AGENTS.md](docs/AGENTS.md) for writing
  instructions)

## Core Behaviors & Patterns

- The runtime uses command-mode primitives and durable `WorkflowStepResult` handles.
- The DSL layer builds author-facing step definitions and forwards them into runtime execution primitives.
- Workflow APIs separate asynchronous step start from blocking result access.
- Payload handling is reducer-based and commonly combines local step input with workflow state.

## Conventions

- Prefer small, explicit adapters between DSL-facing DTOs and runtime command objects.
- Keep primitive naming stable: `execute`, `waitFor`, `modifyPayload`, with blocking variants prefixed by `await`.
- Follow existing JavaDoc style on public APIs and author-facing DSL entry points.
- Avoid broad refactors when a targeted change is enough to preserve current semantics.

## Working Agreements

- Work at a senior software engineering standard: understand surrounding architecture before changing APIs.
- Verify code changes by running relevant automated tests; compile-only checks are not sufficient when tests exist.
- When a change spans runtime or DSL behavior, include `examples/simple` and `examples/itest` in verification because it
  acts as the in-memory integration suite. Especially `examples/itest` is important since these tests are used as 
  feature tests regression test suites.
- When a change affects Spring Boot integration behavior, include `examples/bike-rental` in verification.
- The root reactor activates `examples` by default, but `-pl ...` narrows the reactor to the listed projects; if
  examples should be tested, include them explicitly in the project list.
- Do not remove or rewrite existing overloads or adapters unless the task explicitly asks for that cleanup.
- Make sure the public API is clear and consistent. All public methods must be documented with JavaDoc. The Javadoc for
  the method must start with a sentence. The parameter and return descriptions should be statements without trailing
  dot.
- Make sure every file contains a license header.
- Use consistent indentation and formatting throughout the codebase.
