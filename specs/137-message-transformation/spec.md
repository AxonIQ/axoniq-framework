# Feature Specification: Event Transformation API

**Feature Branch**: `enhancement/137/spec-message-transformator`

**Created**: 2026-05-18

**Status**: Draft

**Input**: User description: "Transformation API for Axoniq Framework 5.2.0
(issue AxonIQ/axoniq-framework#137; ported from AxonIQ/AxonFramework#3597)"

---

## Background: Why Events Need Versioning

In an event-sourced system, every change in the application is recorded as an event and stored permanently. Events are never changed after they are written. They are immutable facts about what happened.

This immutability is a strength (complete audit trail, ability to replay history), but it creates a challenge when your application evolves: old events stored in the past must still work with new code.

Axon Framework 5 addresses this with two complementary mechanisms that run in order when a message is read:

1. A **message transformer** changes the *structure* of a message: splitting one event into two, renaming a message type, or reshaping a payload. It runs at message ingress before any handler sees the message, so every handler observes the same, up-to-date shape. The same mechanism applies uniformly to events (5.2.0), commands and queries (5.3+, see US8 / US9), regardless of whether the message arrived locally or from a remote node. Snapshots remain architecturally compatible (deferred for 5.2.0, see Part C). Implementation wiring is documented in [plan.md](plan.md).

2. A **message converter** changes the *representation* of the payload, producing the concrete type a handler declared. It runs after the transformer, on the already-restructured message. The converter handles many common versioning scenarios automatically: for example, when you add a new optional field to an event class, old stored events simply receive a default value for that field when they are read. You do not need to do anything.

You reach for a transformer only when the message itself must be restructured before any handler sees it. The converter cannot handle these on its own.


## User Scenarios & Testing

This section is split into three parts:

- **Part A**: scenarios the converter handles on its own. No transformer is needed.
- **Part B**: scenarios that require a transformer. These are the stories that drive the new API.
- **Part C**: scenarios that are out of scope or where a transformer is the wrong tool entirely.
  Each entry explains what to do instead and why a transformer does not solve the problem.

---

### Part A: What the converter handles (no transformer needed)

The converter absorbs most schema evolution on its own through two mechanisms: **payload
conversion at handling time** (each handler declares its preferred Java type and the converter
produces it from the stored payload) and **converter-level compatibility configuration** (Jackson
annotations, Avro reader/writer schemas, JAXB bindings, defaults, ignore-unknown settings, all
on the converter, never on the transformation chain).

So **whether a change requires a transformer depends as much on how your converter is configured
as on the change itself.** A loose configuration absorbs more, a strict one rejects more.

> **The rule:** configure your converter first, reach for a transformer only when the change
> cannot be expressed there. The decision below covers every common case.

---

#### Do I need a transformer?

**Does the stored message need to be restructured before any handler sees it?**

##### No, the converter handles it natively

*Field changes:*

- **Add OPTIONAL field**: converter supplies default for missing fields (Jackson `null`, Avro schema default, JAXB library default).
- **Remove a field** (optional or required), converter ignores unknown fields (Jackson `@JsonIgnoreProperties(ignoreUnknown=true)`, Avro writer/reader schema resolution, JAXB tolerates unknown elements).
- **Change required -> optional** (relax), schema / annotation update.
- **Rename a field**: old-name -> new-name mapping (Jackson `@JsonProperty("oldName")`, Avro `"aliases": ["oldName"]`, JAXB `@XmlElement(name="oldName")`).
- **Compatible type change** (e.g., `int -> long`), converter coerces (Jackson numeric coercion, Avro schema promotion).

*Class / format / handler changes:*

- **Rename Java class only** - `@Event(name = "OldName")` preserves `MessageType` routing, payload unchanged.
- **Switch serialization format** - reconfigure `EventConverter`, event classes unchanged.
- **Handler wants different representation**: each handler declares its preferred type (a JSON tree, an Avro generic record, an XML document, a concrete class).

##### Yes, transformer needed

*Payload can't be bridged at the converter level:*

- **Add REQUIRED field, no usable default** -> 1:1 structural transformation (derive value from existing payload).
- **Change optional -> required, no usable default** -> 1:1 structural transformation.
- **Rename triggers payload restructure beyond a simple alias** -> 1:1 structural transformation.
- **Incompatible type change** (e.g., `String -> int` requiring a parse step) -> 1:1 structural transformation.
- **Payload restructure** (one field -> 2, combining fields, complex shape) -> 1:1 structural transformation.
- **Strict converter config rejects an otherwise-bridgeable difference** (`FAIL_ON_UNKNOWN_PROPERTIES = true`, Avro `NONE`, JAXB without an alias) -> relax the config, or a 1:1 structural transformation.

*Identity / cardinality changes:*

- **Event identity changed (name or version), payload unchanged** -> 1:1 rename (no payload mapper).
- **Event identity changed, payload also changes** -> 1:1 structural transformation.
- **One event becomes multiple events** -> 1:N split transformation.
- **Drop event entirely** -> 1:0 drop transformation.

---

#### Tests proving the No-branch leaves

Each NO-branch leaf is a `@Nested` class of the same name in [`PayloadConversionCapabilityTest`](../../examples/university-demo/src/test/java/org/axonframework/examples/demo/university/faculty/PayloadConversionCapabilityTest.java) (Jackson 2, full AF5 pipeline, also the sole home of `RenamedJavaClass`, which is format-independent), [`CborPayloadEvolutionCapabilityTest`](../../conversion/src/test/java/org/axonframework/conversion/jackson/CborPayloadEvolutionCapabilityTest.java) (Jackson 3 / CBOR), and [`AvroPayloadEvolutionCapabilityTest`](../../conversion/src/test/java/org/axonframework/conversion/avro/AvroPayloadEvolutionCapabilityTest.java) (Avro). "Change required -> optional" has no separate test, it is a converter config change with no framework behaviour to assert.

---

### Part B: When Transformations Are Needed

Payload conversion cannot handle the scenarios below. They require registering a transformation because
the stored event stream itself must change. Each story is a distinct, independently testable use case.

---

#### User Story 1: Structural Payload Transformation (Priority: P1)

**Plain-English explanation**: `CourseCreated` had a single `capacity` field, you now need
`minCapacity` and `maxCapacity`. Thousands of v1.0.0 events are already stored. A single
transformation converts v1.0.0 to v2.0.0 at read time, so every handler (projections,
event-sourced entities) sees the new structure without per-handler conversion logic.

**Why this priority**: the most common reason to write a transformer, every other story builds
on this foundation.

**Acceptance Scenarios**:

1. **Given** a stored `CourseCreated` event at version 1.0.0 with a single `capacity` field,
   **When** an event handler processes it after the transformation is registered,
   **Then** the handler receives a `CourseCreated` event at version 2.0.0 with `minCapacity` and
   `maxCapacity` derived from the original `capacity`, and the old `capacity` field is absent.

2. **Given** multiple handlers registered for the same event,
   **When** the event is read from storage,
   **Then** all handlers receive the transformed payload, no handler needs to implement its own
   conversion.

3. **Given** an event that does not match the transformation's target name and version,
   **When** it is read from storage,
   **Then** it passes through the chain unchanged.

4. **Given** the same stored `CourseCreated` v1.0.0 event and the same registered transformation,
   **When** the event is read in three different contexts, (a) loading an event-sourced entity,
   (b) a DCB read enforcing command-side consistency, and (c) a tracking processor building a
   projection --
   **Then** all three contexts receive the identical transformed `CourseCreated` v2.0.0 payload.
   The reading context MUST NOT affect the transformation outcome.

5. **Given** a stored `CourseCreated` event that carries no version (written before versioning was
   introduced to the system),
   **When** a transformation registered for `com.example.CourseCreated` at version `0.0.1` is active,
   **Then** the transformation applies to that event. The framework treats the absence of a version
   as version `0.0.1`, the AF5 default.

6. **Given** a single registered `CourseCreated` v1.0.0 -> v2.0.0 transformation and the same stored
   event read simultaneously by multiple threads (e.g. two tracking processors and an event-sourced
   entity load running in parallel),
   **When** the framework invokes the transformation function concurrently from those threads,
   **Then** every invocation produces an identical v2.0.0 payload, and the framework MUST NOT
   require external synchronization from the transformation author. The transformation is treated as
   a static function. The framework does not serialize invocations per stream, per processor, or
   per entity.

---

#### User Story 2: MessageType Change / Rename (Priority: P2)

**Plain-English explanation**: an event's `MessageType` (`QualifiedName` + version) needs
to change while the payload stays the same. Common cases: rename the event (e.g., `CourseOpened`
becomes `CourseCreated` after a domain-modelling refinement), bump only the version, or change
both name and version. Declare a rename from the old `MessageType` to the new one, no payload
function needed, and handlers registered for the new `MessageType` receive the renamed events
with the payload passed through unchanged.

**Why this priority**: `MessageType` changes are common during domain modelling refinements;
only the label changes, not the data, so the registration should require minimal effort.

**Acceptance Scenarios**:

1. **Given** a stored event with `MessageType (com.example.CourseOpened, 1.0.0)`,
   **When** a rename to `(com.example.CourseCreated, 1.0.0)` is declared and a handler
   registered for the new `MessageType` processes the event stream,
   **Then** the handler receives the event with the original payload unchanged.

2. **Given** a rename from `CourseOpened` to `CourseCreated` is declared,
   **When** any handler still registered for the old `MessageType` is evaluated,
   **Then** it does not receive the renamed event (the old `MessageType` no longer matches
   after transformation).

3. **Given** only a version bump is needed (same `QualifiedName`, version incremented
   from `1.0.0` to `2.0.0`),
   **When** a version-only rename is declared,
   **Then** handlers registered for the new version receive the event with the original payload.

---

#### User Story 3: Event Splitting (Priority: P3)

**Plain-English explanation**: an event like `StudentEnrolledAndCourseUpdated`
bundles two facts into one. Split it into `StudentEnrolled` followed by `CourseCapacityUpdated`
so handlers/projections written for each event work correctly. The transformation
produces both replacement events in declared order every time the original is read.

**Why this priority**: splitting changes the shape of the stream itself, per-handler split
logic would be duplicated everywhere, one transformation is applied transparently for all
consumers.

**Acceptance Scenarios**:

1. **Given** a stored `StudentEnrolledAndCourseUpdated` event at version 1.0.0,
   **When** the event stream is read with the splitting transformation registered,
   **Then** two events appear in its place, `StudentEnrolled` first, then `CourseCapacityUpdated` --
   in that exact order.

2. **Given** a handler registered only for `StudentEnrolled`,
   **When** the split produces both events,
   **Then** that handler receives only the `StudentEnrolled` event and is unaware of
   `CourseCapacityUpdated`.

3. **Given** an event of a different type in the same stream,
   **When** it is read,
   **Then** it passes through the splitting transformation unchanged.

4. **Given** a split transformation for `StudentEnrolledAndCourseUpdated` v1.0.0 and a subsequent
   transformation for `StudentEnrolled` v1.0.0 registered in that order,
   **When** the stream is read,
   **Then** the split fires first producing `StudentEnrolled` v1.0.0 and `CourseCapacityUpdated` v1.0.0,
   and the subsequent transformation then applies to the `StudentEnrolled` output event, both output
   events from the split flow through the remainder of the chain.

5. **Given** a stored `StudentEnrolledAndCourseUpdated` event with sequence number S and tracking
   token T,
   **When** the split transformation produces N output events,
   **Then** all N output events share sequence number S and tracking token T. The framework MUST
   NOT renumber. The order among the N outputs is the order returned by the transformation
   function.

---

#### User Story 4: Event Dropping (Priority: P4)

**Plain-English explanation**: a `SystemHeartbeat` event was accidentally stored and carries no
business meaning. Register a drop transformation (one event in, zero events out, the degenerate
case of splitting) so no handler sees it, instead of putting "ignore this" logic in every handler.

**Why this priority**: keeps streams clean and removes defensive skip-logic scattered across
handlers. Also matters for compliance: some events must be suppressed from processing without
physically deleting them from storage.

**Acceptance Scenarios**:

1. **Given** a stored `SystemHeartbeat` event,
   **When** the event stream is read with the drop transformation registered,
   **Then** no output event appears in its place and no handler receives it.

2. **Given** a mix of `SystemHeartbeat` and `CourseCreated` events in the same stream,
   **When** the stream is read,
   **Then** only `CourseCreated` events reach handlers, every `SystemHeartbeat` is silently suppressed.

3. **Given** a tracking processor that has processed up to and including a `SystemHeartbeat` event,
   **When** the processor restarts,
   **Then** the processor resumes after the dropped `SystemHeartbeat`. It does not reprocess it.
   The stream position (tracking token) advances past dropped events as if they had been handled.

---

#### User Story 5: Chaining Transformations Across Multiple Versions (Priority: P2)

**Plain-English explanation**: `CourseCreated` evolves through v1 (`capacity`) -> v2
(`minCapacity` + `maxCapacity`) -> v3 (`capacityRange` value object). Following Gregory Young,
write one small transformation per version hop (v1->v2, v2->v3) rather than a single v1->v3
jump. Register them in order, the framework chains them automatically, a v1.0.0 event passes
through v1->v2 first, then v2->v3, arriving at the handler as v3.0.0. When v4 arrives, add one
v3->v4 transformation, existing ones stay untouched.

**Why this priority**: chaining keeps transformation maintainable over time, otherwise every new
version requires updating all previous transformations.

**Acceptance Scenarios**:

1. **Given** a stored `CourseCreated` event at version 1.0.0,
   **And** a v1->v2 transformation and a v2->v3 transformation registered in that order,
   **When** the event is read from storage,
   **Then** the handler receives the event as version 3.0.0 with the correct transformed payload,
   and neither intermediate version (2.0.0) is exposed to any handler.

2. **Given** a stored `CourseCreated` event at version 2.0.0 (written after the first migration),
   **When** the same chain is applied,
   **Then** the v1->v2 transformation does not apply (version does not match), the v2->v3 transformation applies,
   and the handler receives version 3.0.0.

3. **Given** a stored `CourseCreated` event at version 3.0.0 (written after both migrations),
   **When** the same chain is applied,
   **Then** neither transformation applies and the handler receives the event unchanged.

4. **Given** two transformations registered in the wrong order (v2->v3 before v1->v2),
   **When** a v1.0.0 event is read,
   **Then** the v2->v3 transformation does not match v1.0.0 (input version does not match),
   the v1->v2 transformation applies, and the framework's fixed-point iteration immediately
   restarts and applies v2->v3, so the handler receives version 3.0.0. Ordering is the user's
   responsibility for concrete `MessageType` `from` matches; the framework does not validate
   it.

#### User Story 6: Feedback on Misconfiguration and Runtime Failures (Priority: P1)

**Plain-English explanation**: developers misconfigure transformations in predictable ways:
duplicate `from` identity (copy-paste mistake), `from == to` (infinite loop), two 1:1
transformations forming a cycle. The framework catches these BEFORE any event is read.
Duplicate and self-loop on concrete `MessageType` `from` are detected at registration time;
multi-step cycle on the graph of concrete `from -> to` edges is detected at chain lock time.
Predicate-based cycles cannot be detected statically and are caught at runtime by an iteration
cap. Runtime transformation failures (exceptions thrown during processing) also propagate
immediately with full context, no silent skip.

Naming mistakes (e.g., a `from.qualifiedName()` that does not match the names produced by the
configured `MessageTypeResolver`) cannot be detected at startup, the framework has no
knowledge of which `MessageType`s will appear in the stream. Such mismatches silently
pass-through (FR-005). They are the developer's responsibility to verify (see FR-017).

**Why this priority**: silent misconfiguration is the highest-cost failure mode for a
startup-only API. A clear error at startup costs nothing, a runtime corruption of projections
takes hours to diagnose.

**Acceptance Scenarios**:

1. **Given** two transformations registered with the same `from` identity (same `QualifiedName`
   AND same version),
   **When** the second registration is attempted,
   **Then** the framework raises an error at registration time identifying the conflicting `from`
   value, and no event processing occurs.

2. **Given** a 1:1 transformation where `from` and `to` are identical (same name AND same version),
   **When** the registration is attempted,
   **Then** the framework raises an error at registration time identifying the self-loop, and no
   event processing occurs.

3. **Given** two 1:1 transformations forming a cycle (transformation A maps `X@1.0.0 -> X@2.0.0`
   and transformation B maps `X@2.0.0 -> X@1.0.0`),
   **When** the chain is locked,
   **Then** the framework raises an error at lock time enumerating the full cycle
   (`X@1.0.0 -> X@2.0.0 -> X@1.0.0`), and no event processing occurs. The framework MUST NOT attempt
   to silently break the cycle or apply only part of it.

4. **Given** a correctly configured transformation chain that has already started processing events,
   **When** an attempt is made to register an additional transformation at runtime,
   **Then** the framework rejects the registration immediately with a clear error stating that the
   chain is immutable once event processing has begun, and the chain remains unchanged.

5. **Given** a transformation whose function throws an exception when applied to a specific event,
   **When** that event is read from the event store,
   **Then** the framework propagates the exception immediately to the caller, the event-sourced entity load,
   DCB read, or tracking processor, halting processing. The exception clearly identifies which
   transformation failed and the name, version, and stream position of the event that triggered it.
   The framework MUST NOT silently skip the failed event or log-and-continue.

6. **Given** a 1:1 transformation declared with `to = (com.example.CourseCreated, 2.0.0)` whose
   function returns a payload of a different type (e.g. `OrderPlacedV2`) or a payload whose
   resolved `MessageType` does not match the declared `to` (e.g. version `3.0.0` or name
   `com.example.CourseScheduled`),
   **When** the event is read from the event store and the transformation is applied,
   **Then** the framework propagates a runtime failure under the same path as scenario 5, with a
   message identifying the offending transformation, the declared `to`, and the actual output
   `MessageType`. The framework MUST NOT silently coerce the output identity to match the declared
   `to`, and MUST NOT let the wrong-typed output flow into the rest of the chain.

---

#### User Story 7 - Startup Observability for the Transformation Chain (Priority: P2)

**Plain-English explanation**: a developer needs to confirm at deploy time that registered
transformations were actually picked up by the framework, and during an incident needs
detailed per-event logging to diagnose whether the chain caused incorrect projection state.
DEBUG at startup confirms wiring, TRACE per-application traces execution. Observability is
opt-out: developers MAY disable all chain logging on performance-critical paths.

**Why this priority**: registration is a startup-time concern. A developer cannot inspect the
chain at runtime the way they can inspect a handler registry. Log output is the only way to
confirm correct wiring in production.

**Acceptance Scenarios**:

1. **Given** two transformations registered at startup,
   **When** the transformation chain is built,
   **Then** a single DEBUG-level log entry is emitted listing the number of registered transformations
   and their `from` identities.

2. **Given** a stored event that matches a registered transformation,
   **When** the transformation is applied,
   **Then** a TRACE-level log entry is emitted identifying which transformation ran and the name and
   version of the event it was applied to.

3. **Given** a stored event that does not match any registered transformation,
   **When** it passes through the chain,
   **Then** no TRACE log entry for a transformation being applied is emitted for that event (the
   chain is silent for non-matching events at TRACE level).

4. **Given** a chain built with observability disabled,
   **When** transformations are registered and applied,
   **Then** no log entries are emitted by the chain at any level (neither startup DEBUG nor
   per-applied TRACE).

---

#### User Story 8 - Command Transformation (Priority: P3) `[Delivery: 5.3+]`

**Delivery note**: Per scope decision 2026-05-21, command transformation is held back to a 5.3+
release; the 5.2.0 issue (#137) delivers events only. This user story remains in the spec as
the design target for the future `axoniq-message-transformation-cqrs` module.

**Plain-English explanation**: a receiver applies the transformation chain to an incoming
command before dispatching it to the command handler. The mechanism is the same as for
events. For commands the chain MUST fire on every incoming command that reaches a handler --
whether it arrived from a remote node or via a local-process dispatch. Common scenarios:
structural field changes, renames, version bumps. Most relevant in rolling deployments where
old and new service versions coexist. Decoration wiring is documented in [plan.md](plan.md).

**Why this priority**: deliverable on top of the event transformation infrastructure. 1:1 only (1:N
split / 1:0 drop do NOT apply, each command is a single intent expecting a response).
Sender-side transformation (new-to-old) is deferred (see Part C).

**Acceptance Scenarios**:

1. **Given** a v1 `EnrollStudent` command without an `enrollmentReason` field, and a 1:1
   transformer registered that fills a default `enrollmentReason`,
   **When** the command is dispatched,
   **Then** the v2 handler receives the transformed payload with `enrollmentReason` populated.

2. **Given** a developer attempts to register a 1:N or 1:0 (split/drop) transformer for a
   command,
   **When** registration is attempted,
   **Then** the framework rejects it at registration time with a clear error stating that split
   and drop do not apply to commands.

---

#### User Story 9 - Query Transformation (Priority: P3) `[Delivery: 5.3+]`

**Delivery note**: Per scope decision 2026-05-21, query transformation is held back to a 5.3+
release; the 5.2.0 issue (#137) delivers events only. This user story remains in the spec as
the design target for the future `axoniq-message-transformation-cqrs` module.

**Plain-English explanation**: a receiver applies the transformation chain to an incoming
query before dispatching it to the query handler, same mechanism as commands and events.
The chain MUST fire on every incoming query that reaches a handler (local-routed or remote).
Common scenario: a new optional filter parameter (e.g., `includeArchived`) is added and the
handler needs a default value for queries sent by older callers.

**Why this priority**: deliverable on top of the same transformer mechanism.
1:1 only (1:N split / 1:0 drop do NOT apply). Sender-side transformation is deferred (see Part C).

**Acceptance Scenarios**:

1. **Given** a v1 `FindCoursesByFaculty` query without an `includeArchived` parameter, and a
   1:1 transformer registered that fills `includeArchived = false`,
   **When** the query is dispatched,
   **Then** the v2 handler receives the transformed payload with `includeArchived` populated.

2. **Given** a developer attempts to register a 1:N or 1:0 (split/drop) transformer for a
   query,
   **When** registration is attempted,
   **Then** the framework rejects it at registration time with a clear error stating that
   split and drop do not apply to queries.

### Part C: Out of Scope and Non-Transformer Scenarios

This section documents scenarios that are either deferred to a later release, or are not a
transformer problem at all. Knowing when NOT to reach for a transformer is as important as
knowing when to use one.

The entries fall into two categories:

- **Deferred**: a transformer is the right concept, but the scenario is out of scope for 5.2.0. The transformer API shipped in 5.2.0
  is designed so these can land in a later release without breaking changes for existing users.
- **Wrong tool**: a transformer would give the wrong answer or corrupt the audit trail. The
  correct solution is a different pattern entirely (a new event type, a stateful projection, a
  Copy and Replace migration, etc.).

---

#### N-to-1 Merge `[Deferred]`

**Scenario**: combine multiple (N) stored events into one (1) output event, for example,
merge `StudentRegistered` + `StudentEnrolledInFaculty` into a single `StudentJoined` event.

**Why deferred for 5.2.0**: merging requires the framework to "remember" earlier events while
reading later ones. Two unresolved problems:

1. **Memory scope is context-dependent.** Entity loads are bounded streams (per-entity, finite);
   tracking processors are unbounded streams (every entity, continuous). Memory that resets
   per-entity is predictable for the first, cross-entity memory in the second would silently
   mix unrelated entities, exactly the AF4 context-aware-transformer bug class the team has
   chosen not to repeat.

2. **`MessageStream` has no grouping / windowing today.** Adding it would touch core streaming
   infrastructure well outside this feature's scope.

**What to do instead**:

- **Stateful projection**: accumulate events over time and produce a combined read model. Right
  tool for most cases.
- **Copy and Replace** (Gregory Young), one-time migration: read old events, write new ones
  to a fresh stream. Right tool when the stored stream itself must change.
---

#### Moving Data Between Events `[Deferred]`

**Scenario**: enrich a stored event with data captured from an earlier event in the stream, for
example, when reading `TuitionPaid`, attach the `scholarshipCode` that was present on the earlier
`StudentEnrolled` event of the same student.

**Why deferred for 5.2.0**: this requires a **context-aware transformation**: one that
remembers data from earlier events and uses it when transforming a later event. Same memory-scope
problem as the N-to-1 entry above, applied at the field level: per-entity scope works for entity
loads but silently mixes data across entities on tracking processors. AF4 context-aware transformers
had this exact inconsistency, causing subtle and hard-to-reproduce bugs.

**What to do instead**: **Copy and Replace**: read existing events, rewrite them with the
fields they should have carried, switch the application over to the corrected stream. One-time
migration, not ongoing transformation.

---

#### Sender-side transformation `[Deferred]`

**Scenario**: in a rolling deployment, a newer sender strips fields that an older receiver does
not yet understand. For example, service v2 sends a `FindCoursesByFaculty` query with a new
`includeArchived` filter, service v1's handler does not know the field. Ideally the sender
removes it before dispatch.

**Why deferred for 5.2.0**: sender-side transformation is new-to-old at the
sender, instead of old-to-new at the receiver. It introduces sender-awareness questions (how
does the sender know what version the receiver understands?) that need their own specification.

Command and query transformation (the receiver-side, old-to-new direction) is in scope for the
overall design -- see US8 (commands) and US9 (queries) in Part B -- but delivery is held to
5.3+ per scope decision; 5.2.0 ships events only. The transformer applies uniformly at every
message-ingress point regardless of whether the message arrived from a remote node or was
dispatched locally; wiring details are in [plan.md](plan.md).

**When to revisit**: when concrete rolling-deployment cases surface that cannot be solved by
receiver-side transformation alone.

---

#### Snapshot Transformation `[Deferred]`

**Use case**: a 1:1 `Snapshot -> Snapshot` transformer would let the framework apply a
state-schema change to a stored snapshot instead of discarding it and replaying all events.

**Why deferred for 5.2.0**: scope and focus, NOT architecture. Snapshot entries flow inline
through the same event stream the chain reads, so a developer who registers a matching
transformation in 5.2.0 will see it fire on snapshots; what is deferred is the user-facing
API/docs/test fixtures, not the chain wiring. Wiring details are in [plan.md](plan.md).

**Ergonomic gap (not a blocker)**: a snapshot's payload accessor in the public surface today
requires going through the `Converter`; a typed accessor on `Snapshot` may be added later.
Both options remain open.

**When justified** (Gregory Young): snapshots are a cache, not a source of truth.
Discard-and-replay is the correct primary strategy, snapshot transformation is the optimisation,
worth it only when the event history is huge AND replay is slow AND the snapshot schema changed.

---

#### Annotation-Based Transformation Registration `[Deferred]`

In 5.2.0, transformations are registered programmatically only. An annotation-based model is intentionally deferred.

**Why not now**: The spec's primary AF4 complaint is that Spring Bean-based registration made chain
order unpredictable. Introducing an annotation-discovery mechanism in the same release risks
reintroducing that problem through a different path. Programmatic registration keeps the ordering
guarantee trivially enforceable: the order of the API calls is the chain order, with no scanning,
no priority attributes, and no framework magic.

**Forward direction (informational)**: when annotations return, ordering MUST be expressed
through an explicit `EventTransformationChain` registry or equivalent bean, not inferred
from bean-discovery order. This preserves the deterministic-order guarantee that programmatic
registration provides today.

#### Semantic Meaning of a Field Changed Silently `[Wrong tool]`

**Scenario**: a field's name and type stay the same, but its meaning changes. For example,
`CourseCreated.capacity` used to mean "total available seats" but should now mean "seats
remaining after cancellations", same field, different interpretation.

**Why a transformer is the wrong tool**: the stored data did not change, only its meaning did.
A transformer that rewrites old events to match the new interpretation corrupts the audit trail:
events that were correct under the old meaning would be silently presented as if they had always
carried the new number.

**What to do instead** (Gregory Young's rule): a value that now means something different from
what it meant when it was written is a different event type, not a new version. Create a new
event with a name that makes the new meaning explicit, e.g., `CourseRescheduled` with an
`availableSeats` field. Keep `CourseCreated.capacity` meaning what it always meant.

---

#### New Event Cannot Be Derived from the Old One `[Wrong tool]`

**Scenario**: an event has changed so fundamentally that the new version cannot be computed
from the old stored data. For example, `CourseCreated` was originally modeled around rooms
and manual time slots, the new model uses recurring templates and automated slot generation --
a different concept, not a new version.

**Why a transformer is the wrong tool**: a transformer must produce the new event from the old
data. If the old data lacks enough information, the transformer is forced to invent or default
values, producing events that misrepresent what actually happened and destroying the audit
trail.

**What to do instead** (Gregory Young's rule): a new event version MUST be derivable from the
old version. If it cannot, it is a genuinely new business event, give it a new name (e.g.,
`CourseScheduled`). Run both event types in parallel during migration: old projections continue
to consume `CourseCreated`, new projections consume `CourseScheduled`. Use Copy and Replace as
a last resort if the old stream must be fully replaced.

---

## Requirements

- **FR-001 (1:1 transformations)**: Developers MUST declare a `from` identity (a `MessageType`,
  i.e. `QualifiedName` + `version`) and a `to` identity (same shape), plus an optional payload
  mapper. With no mapper the payload passes through unchanged. The mechanism applies to any
  message type (event, command, query).
  _Traces to: US1, US2._
- **FR-002 (Rename)**: A pure rename is FR-001 with no payload mapper, no separate API needed.
  _Traces to: US2._
- **FR-003 (1:N / 1:0 transformations)**: For splits and drops, developers MUST declare only the
  `from` identity together with a mapper that produces 0..N replacement events. Each replacement
  carries its own identity and payload as the mapper determines, the framework imposes no
  constraint on output identities for this pattern. Self-loop detection (FR-008) does not apply
  since no `to` identity is declared.
  _Traces to: US3, US4._
- **FR-004 (Ordering and lifecycle)**: Registration order MUST be the chain application order, the
  framework MUST NOT reorder. Registration is programmatic and valid only at application startup;
  the chain locks once event processing begins. Late registration MUST be rejected with a clear
  "chain is locked" error. Annotation-based registration is out of scope for this release.
  _Traces to: US1, US5, US6 scenario 4._
- **FR-005 (Matching)**: A transformation declares `from` either as a concrete `MessageType`
  (default: exact-equals comparison on `QualifiedName` + `version`) or as a `Predicate<MessageType>`
  (user-supplied: e.g. range, regex, semver). Non-matching events pass through unchanged. Naming
  consistency between `from` identities and the names produced by the configured
  `MessageTypeResolver` is the user's responsibility.
  _Traces to: US1 scenario 3, US5._
- **FR-006 (Determinism and thread-safety)**: Transformations MUST be deterministic and
  thread-safe, same input always produces the same output, and the framework MAY invoke them
  concurrently from any thread. Transformations MUST NOT call external services or databases,
  depend on time/randomness, or mutate shared state. They MAY read in-process immutable data
  (e.g., a constant lookup table). This is a contract for documentation. The framework does not
  enforce it at runtime.
  _Traces to: US1 scenarios 4 and 6, US5 scenario 1._
- **FR-007 (Chain composition)**: The framework MUST process each incoming message by
  fixed-point iteration over the registered transformations:
  1. Walk the registrations in registration order.
  2. First transformation whose `from` matches the current message wins; invoke it.
  3. For 1:1 output: replace the current message with the output, restart from step 1.
  4. For 1:N output: each output re-enters the chain independently (recursive step 1).
  5. For 1:0 (drop) output: terminate with empty result.
  6. If no transformation matches in step 1: terminate; emit the current message.

  This subsumes both same-name version bumps (e.g. v1 -> v2 -> v3 of `CourseCreated`) and
  cross-name renames (e.g. `CourseOpened` -> `CourseCreated`) without special-casing.
  _Traces to: US1 scenario 4, US2, US3 scenario 4, US5._
- **FR-008 (Conflict detection)**: The framework MUST detect:
  - **Duplicate `from`** (two 1:1 transformations with the same concrete `(name, version)`):
    raised at registration time.
  - **Self-loop** (concrete `from == to`): raised at registration time.
  - **Multi-step cycle** in the graph of concrete `from -> to` edges (e.g. `X@1.0.0 -> X@2.0.0`
    then `X@2.0.0 -> X@1.0.0`): raised at chain lock time with the full edge list.

  The framework includes a defensive runtime safety bound to prevent infinite loops from
  pathological misconfiguration that cannot be detected statically (e.g. two overlapping
  predicate `from`s producing each other's input); the exact bound is implementation-defined.
  _Traces to: US6._
- **FR-009 (Typed payload access)**: Each transformation declares a target Java type at
  registration; the framework converts the stored payload to that type via the registered
  `Converter` before invocation, then passes the converted payload plus the active
  `ProcessingContext` (nullable) to the mapper. Mapper shape:
  `BiFunction<T, ProcessingContext, U>` for 1:1, `BiFunction<T, ProcessingContext, List<TransformedEvent>>`
  for 1:N. Typical input types: structured representations (JSON tree, Avro generic record,
  POJO) or raw bytes. Conversion is on-demand (no pre-conversion). A future overload giving
  the mapper direct `Converter` access (for multi-view transformations) is deferred to 5.3+.
  _Traces to: US1, US3, US4._
- **FR-010 (Envelope and metadata)**: The message envelope, entity type, entity identifier,
  tracking token, sequence number, MUST be preserved unchanged by every transformation, any
  attempt to modify it is overridden by the framework before delivery. Metadata MAY be modified
  via the message-level entry point (the transformation returns a complete `EventMessage`), the
  payload-only entry point carries metadata forward unchanged. For 1:N splits, every output event
  inherits the input's envelope unchanged. The framework MUST NOT renumber: all N outputs share
  the input's tracking token and sequence number. Per-output metadata MAY differ when the rule
  provides distinct values. The event store remains append-only, modifications only affect the
  in-memory `EventMessage` flowing downstream.
  _Traces to: US1 scenario 1, US3 scenario 1, US3 scenario 5._
- **FR-011 (Lazy deserialization and chain cost)**: The framework MUST NOT deserialize a payload
  unless the event matches at least one registered transformation. Non-matching events MUST pass
  through without conversion. Per-event work on the non-matching path MUST be O(1) for
  transformations whose `from` is a concrete `MessageType` (via a `QualifiedName`-keyed index)
  and O(P) for transformations whose `from` is a `Predicate<MessageType>`, where P is the number
  of predicate-based transformations the user has registered. No per-event allocations on either
  path. Benchmark thresholds and JMH targets are set in [plan.md](plan.md).
  _Traces to: US1 scenario 3, US4 scenario 2._
- **FR-012 (Reading-context consistency)**: The chain MUST be applied identically across all
  three event-reading contexts: event-sourced entity loads, DCB reads (`SourcingCondition`),
  and tracking processor reads. Storage-engine filtering (sourcing/streaming conditions,
  consistency-marker / terminal-event bookkeeping) runs BEFORE the chain receives the stream;
  tag-based identity is fixed at append time, so a transformation that changes message identity
  does NOT affect which events match, use Copy and Replace if the stored stream itself must
  change. Any `EventStore` implementation -- engine-backed or not -- participates in
  transformation. Wiring details are in [plan.md](plan.md).
  _Traces to: US1 scenario 4._
- **FR-013 (Observability)**: The framework MUST emit one DEBUG entry at chain build listing
  the transformation count and each transformer's `from` (+ `to` for 1:1). That is the only
  framework-emitted log in 5.2.0.

  Per-event tracing / metrics via per-transformer hooks (`.when` + `.onApplied` on each
  transformer, matching the AF4 `SingleEntryUpcaster.canUpcast` / `doUpcast` precedent) is
  **deferred to a later release**. The design target is two functional interfaces on the base
  SPI (so events, commands, and queries inherit them uniformly), added as a pure additive
  surface. The framework MUST NOT take a generic name like "Observability" and MUST NOT
  expose chain-wide hooks on the Builder.

  Note: with FR-005's predicate-based `from`, the most common "skip this transformer for
  this input" use case is already expressible without a separate `.when` hook -- the user
  encodes the condition into the `from` predicate itself.
  _Traces to: US7._
- **FR-014 (Position advances past drops)**: When a transformation drops an event, the tracking
  token MUST still advance. A restarting streaming processor MUST NOT reprocess dropped events.
  _Traces to: US4 scenario 3._
- **FR-015 (Exception propagation)**: If any step in applying a transformation throws, the
  transformation function itself OR the framework's pre-invocation conversion to the declared
  target type (e.g., malformed stored payload). The framework MUST propagate the exception
  immediately to the caller (entity load, DCB read, or processor read) and halt. Silent skipping
  or logging-and-continuing is prohibited. The propagated exception MUST identify the failing
  transformation, the event's name and version, and its stream position. For 1:N splits,
  `MessageStream` is pull-based and lazy, already-emitted siblings (events 0..K-1) stay
  delivered, the stream terminates at the failing element K and the framework does NOT roll back
  earlier emissions. Callers needing atomic-all-or-nothing semantics must layer that on top.
  _Traces to: US6 scenario 5._
- **FR-016 (Legacy unversioned events)**: Events stored without an explicit version MUST be
  treated as version `0.0.1` (the AF5 `MessageType` default). Transformations targeting `0.0.1`
  apply to these events, no special API is required.
  _Traces to: US1 scenario 5._
- **FR-017 (Unit-testability)**: A transformation MUST be invocable from a plain JUnit test
  without an event store, processor, or framework bootstrap. The only permitted dependency is a
  `Converter` instance (and only when the converter-access entry point of FR-009 is used). A full
  test fixture API is deferred, this requirement is an invariant on the transformation API
  itself.
  _Traces to: US1, US3, US4, US5._
- **FR-018 (Output identity check)**: For 1:1 transformations that supply a payload mapper, the
  framework MUST verify after invocation that the output payload's identity matches the declared
  `to`. A mismatch MUST be propagated under FR-015 with full context (declared `to`, actual
  output identity, stream position). The framework MUST NOT silently coerce. The check is
  satisfied trivially for pure renames (FR-002), the framework sets the output identity itself.
  It does NOT apply to 1:N/1:0 transformations, output identities are mapper-determined by
  design, and the developer has full responsibility for each replacement event's identity (the
  framework does not validate that the chosen identity is meaningful or subscribed to).
  _Traces to: US1, US6 scenario 6._
- **FR-019 (Commands and queries)**: The transformer mechanism MUST support commands and queries
  in addition to events, using the same uniform stream-in / stream-out shape. The chain MUST
  fire on every incoming command/query that reaches a handler regardless of whether it arrived
  from a remote node or was dispatched locally; wiring details are in [plan.md](plan.md). The
  1:1 patterns (FR-001, FR-002) apply to all three message types. The 1:N / 1:0 patterns
  (FR-003) apply ONLY to events: commands and queries are single-intent messages, so split and
  drop are unavailable for them at the API surface (a compile-time guarantee) and the framework
  MUST also reject any multi-output / zero-output transformer registered for command or query
  types at chain lock time. Sender-side transformation is out of scope (Part C).
  _Traces to: US8, US9._
- **FR-020 (Version range matching)**: Version ranges (e.g. semver `1.x`, `>=1.0 <2.0`) MAY
  be expressed by supplying a `Predicate<MessageType>` to `from(...)` (see FR-005). The
  framework does not ship a semver helper in 5.2.0; users compose their own predicates or
  pull in a helper that the `axon-common` module is expected to provide as follow-on work
  (see [plan.md](plan.md) "Required axon-framework additions"). The framework does NOT
  validate ordering of overlapping predicates -- registration order = apply order; the user
  owns the ordering choice.
  _Traces to: FR-004, FR-005._
- **FR-021 (Data-protection ordering)**: Data-protection mechanisms (PII redaction, field-level
  masking, payload decryption, etc.) MUST operate downstream of the transformation chain.
  Transformations operate on the unprotected message, protection is applied to the final shape
  the handler receives. AF5 ships no built-in data-protection component, when a developer
  adds one (typically as a `MessageHandlerInterceptor`), it sits after the transformer in the
  read pipeline. This ordering is a framework invariant, not a developer responsibility.
  _Traces to: (no user story, security/threat-model invariant)._

## Key Entities

- **Event transformation**: A single unit of logic targeting one event type at one version. Two
  patterns: **1:1** (`from` + `to` identity + optional payload mapper, structural change or
  rename) and **1:N/1:0** (`from` identity + mapper producing 0..N replacement events, split or
  drop). Detailed contract: FR-001 to FR-003.
- **Transformation chain**: The ordered sequence of registered transformations applied when
  reading messages. Order = registration sequence (FR-004).
- **MessageType / Event identity**: `QualifiedName` (arbitrary non-empty string, typically
  built from a namespace + local name like `com.example.CourseCreated`) + arbitrary non-empty
  version string (e.g., `1.0.0`). Both components always present. Unversioned legacy events
  default to `0.0.1` (FR-016). Registration uses the `QualifiedName` produced by the configured
  `MessageTypeResolver`, mismatches between registered names and stream names silently
  pass-through.
- **`from` predicate**: When `from(Predicate<MessageType>)` is used instead of `from(MessageType)`,
  the predicate decides matching (range, regex, semver, etc.). Composable with a future
  `SemverPredicate` helper expected in `axon-common`.
- **DCB read (`SourcingCondition`)**: Dynamic Consistency Boundary, AF5's mechanism for
  command-side consistency without a fixed aggregate root. Bounded stream that may span multiple
  entities. One of the three reading contexts in FR-012 (alongside entity loads and tracking
  processor reads).

## Success Criteria
- **SC-001 (Order)**: 100% of registered transformations are applied in registration order,
  verifiable across all in-scope use cases. Verifies FR-004.
- **SC-002 (Conflicts)**: Every conflict class in FR-008 is detected and reported before any
  event is processed.
- **SC-003 (Examples)**: All in-scope use cases for the delivered slice are demonstrated in a
  new Maven sub-module under `axon-framework/examples/` (alongside `university-demo`,
  `university-java`, `university-java-springboot-3`, etc.). For 5.2.0 this covers the events
  user stories that ship (US1 always; US2 if it lands; US3, US4, US5, US6, US7 if they land as
  nice-to-haves). Command and query example transformations (US8, US9) join when those user
  stories deliver in 5.3+. The example module depends on
  `io.axoniq.framework:axoniq-message-transformation` and runs with passing tests under
  `./mvnw -Pexamples clean verify` in CI with no manual configuration.
- **SC-004 (Migration)**: A developer migrating AF4 transformers can identify the equivalent AF5
  approach for each of their existing transformers from documentation alone.
- **SC-005 (Decision tree)**: Part A of this spec serves as the Converter-vs-transformation
  decision tree, (a) one Converter-only scenario, (b) one transformation scenario, and (c) the
  boundary rule in one sentence. A developer new to AF5 transformations can classify their own
  scenario from Part A alone. Reproduction in the public reference guide (`docs/`) is follow-on
  work, the spec's Part A is the source of truth.
- **SC-006 (Unit-testability)**: Every in-scope transformation example is unit-tested with a
  plain JUnit test, no event store, processor, or framework bootstrap. Verifies FR-017.
- **SC-007 (Identity check)**: A 1:1 transformation whose output `MessageType` differs from the
  declared `to` produces a runtime error naming the transformation, the declared `to`, and the
  actual output identity. 100% surface the error, 0% silently coerce. Verifies FR-018.
- **SC-008 (Concurrency)**: A transformation invoked from multiple concurrent threads with
  sufficient iteration count to exercise real thread interleaving produces identical outputs
  across all invocations with no external synchronization. Specific thread and iteration counts
  are set in plan.md. Verifies FR-006.
- **SC-009 (Observability)**: At chain build, one DEBUG entry lists every registered
  transformation. With no transformations registered for a given event, no per-event
  allocation occurs on the non-matching path. Verifies FR-013 (5.2.0 scope; per-transformer
  hooks deferred).

## Assumptions

- The primary actor is an application developer building an event-sourced system with Axon
  Framework 5, comfortable with Java and basic event-sourcing concepts.
- This feature ships as a commercial AxonIQ Framework module. Pure Axon Framework users do not
  get transformation, they must add the `axoniq-framework` dependency. `axon-framework` itself
  is untouched except for one small additive change needed by the chain implementation; details
  are in [plan.md](plan.md).
- Transformations apply at read time only, the event store is append-only and stored events are
  never modified on disk.
- The demo lives as a new Maven sub-module under `axon-framework/examples/` (plain Java, no
  Spring), alongside the existing example sub-modules. It depends on
  `io.axoniq.framework:axoniq-message-transformation`. Spring Boot integration is follow-on
  work, potentially mirrored in the existing `university-java-springboot-3` /
  `university-java-springboot-4` examples.

## Addendum: Design Principles

Some principles taken into account for writing spec file below

### Simpler than AF4 -- no `IntermediateEventRepresentation`

The new transformation API MUST NOT reintroduce `IntermediateEventRepresentation` or any
equivalent intermediary abstraction. AF5's `Message` already carries payload, `MessageType`
(name + version), and metadata -- this is sufficient.

Any proposed abstraction MUST be justified by a scenario that cannot be served by
`Message` directly. If no such scenario exists, the abstraction is rejected.

The transformation operation SHOULD build on the existing payload-conversion entry point on
`Message` (extended to support same-Type transforms and a mapping operation, per issue #137)
rather than introducing a new transformation primitive.

### Single Responsibility per Transformer (Uncle Bob -- SRP)

Each transformer class MUST do exactly one transformation and have exactly one reason to
change. Bundling multiple transformations into one transformer is forbidden. A rename
transformer renames; a structural transform restructures; a split splits. Never combined.

This keeps transformers small, readable, independently testable, and composable.

### Prefer Chain over Direct (Gregory Young)

When multiple version steps exist (v1 -> v2 -> v3), prefer chained individual transformers over
a single direct transformer (v1 -> v3). Chains are easier to maintain: adding v4 means writing
one new transformer, not rewriting all direct converters.

Direct transformers (v1 -> v3, skipping v2) are only acceptable when the intermediate version
no longer exists in any live system and the performance gain is measurable and required.

### ES Versioning Decision Guide

Use this table to determine which strategy to apply before reaching for an transformer.
Transformation is not always the right tool. (Source: Gregory Young.)

| Problem | Recommended strategy |
|---|---|
| Adding a new field to an event | Weak schema -- consumers use a default for the missing field. No transformer needed. |
| Renaming a field | Support both names temporarily; drop old name after all old events are processed. |
| Event class name changed | Transformer (rename transform). |
| Payload structure changed | Transformer (1->1 structural transform). |
| One event split into two | Transformer (1->N split). Separate transform from split -- two steps. |
| Multiple events merged into one | Transformer (N->1 merge). |
| Snapshot format changed | Transformer (snapshot transformation) -- deferred from 5.2.0, see Part C. |
| Business logic changed (e.g., tax rate) | Store calculated value at creation time -- not a versioning problem. |
| Semantic meaning of a field changed | New event type, not a new version. Never silently change field semantics. |
| Serialization format changed | `MessageConverter` -- not an transformer. |
| System too large for any migration | Copy-Transform (parallel systems). This is the last resort. |

**Key mindset (Gregory Young)**: Versioning is not an occasional problem. It is a
constant, normal part of building event-sourced systems. Design for it from the start.
