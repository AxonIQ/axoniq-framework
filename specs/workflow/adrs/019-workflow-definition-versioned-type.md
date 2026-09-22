# ADR-019: Workflow Definition Identity Uses VersionedType

- Status: Accepted
- Date: 2026-09-11

## Context

A workflow definition is identified by its qualified name and its definition version. This identity is persisted in
workflow lifecycle-event metadata, retained in workflow state, and used to select a definition during recovery and
outside-in state queries.

`MessageType` also carries a qualified name and version, but it describes an event message's payload type. Using it for
a workflow definition conflates the identity of the workflow with the type of events emitted while it runs.

## Decision

Workflow definition identities use `VersionedType` throughout the workflow API and runtime. This includes
`WorkflowState`, `WorkflowStateQuery`, workflow lifecycle metadata, history projection, lifecycle-event factories, and
workflow-configuration lookup.

Lifecycle events continue to use `MessageType` for their event envelope. The workflow definition identity is stored as
metadata on those events and is reconstructed as a `VersionedType` when state is rehydrated.

Comparisons of definition identities use their qualified name and version, so a caller may provide any
`VersionedType` implementation, including `MessageType` where it is already available.

## Consequences

- Public workflow APIs express the domain identity they require without implying an event payload type.
- Rehydration and queries retain the same name-and-version matching semantics.
- Event construction remains explicitly typed as `MessageType`, preserving Axon Framework event-envelope semantics.
