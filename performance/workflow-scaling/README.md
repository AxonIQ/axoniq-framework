# Workflow engine scaling on Axon Server

Scaling probe of the workflow engine against a real Axon Server DCB event store, run 2026-09-22 on
`poc/tla_dst` (1cee2872bd). Three sweeps, one axis each:

1. **Running instances.** N parked workflows on one node. Start, failover restore, release all, heap.
2. **Running-workflow list.** Fixed 10 running, growing count of finished workflows in the store.
3. **One instance's history.** One workflow with a growing number of execute steps.

`report.html` is the interactive report: a Without / With Snapshots toggle and a before-vs-after strip on
top, sliders below, then the measured sweeps and ranked bottlenecks. Open it in a browser.
`results.csv` holds the raw rows without snapshots, `results-snapshots.csv` the rows with snapshots
(`sweep,size,metric,value,events`).

## Headline

| bottleneck | measured | where |
|---|---|---|
| Running-workflow list replayed per segment, no snapshot | failover 42 s at 200k finished workflows, 249 s at 500k, with 10 running | `EventSourcedRunningWorkflows`, `WorkflowEngine.restoreWorkflowsFor` |
| Every event fanned out to every running instance | release of N: 10 s at 5k, 47 s at 10k, 333 s at 20k | `WorkflowEngine.handle`, `SimpleWorkflowExecution.onEvent` |
| Per-instance replay cliff past 1,000 events | restore 0.1 s at 1,002 events, 5 s at 2,002, 20 s at 10,002 | `SimpleWorkflowExecution` task queue (1,000 slots) |

Not a bottleneck: Axon Server tag reads (2M events in 13 s, 10k events in 49 ms), memory (about
21 KB per parked instance), live step latency (flat at about 23 ms).

## With snapshots

Second run with both entities snapshotting into Axon Server (`AxonServerSnapshotStore`, policy
`afterEvents(500)`), confirmed by reading each snapshot back through the gRPC API
(`list_snapshot_in_server`, `state_snapshot_in_server` rows).

| case | without | with |
|---|---|---|
| failover restore, 50,000 finished workflows, 10 running | 12.5 s | 0.1 s |
| restore 1 instance with 10,002 events | 20.4 s | 0.2 s |
| restore 10,000 parked instances (3 events each) | 1.6 s | 1.1 s |
| release 10,000 parked instances (fan-out) | 47 s | 45 s |

Enabling it on this branch, test-only wiring:

- engine: `WorkflowConfigurationDefaults` registers a `SnapshotPolicy` on both entities when
  `-Daxoniq.workflow.snapshots.afterEvents=N` is set; `EventSourcedRunningWorkflows` got bean accessors,
  `EventSourcedWorkflowState` a plain-values `Memento` with `toMemento()` / `fromMemento()`.
- test: `-Dperf.snapshots=true` shares an Axon Server backed `SnapshotStore` across the failover nodes
  (`WorkflowScalingPerfTest.ServerSnapshots`) and records the restore of a second successor, since
  snapshots are created while sourcing.

The production version of this feature is being built separately from `main`
(branch `feature/workflow-snapshots`). The wiring here exists only to measure.

## Harness

`integrationtests/src/test/java/io/axoniq/framework/integrationtests/workflow/WorkflowScalingPerfTest.java`
on top of the Axon Server rig in `DcbFencingBackends` (Testcontainers, `docker.axoniq.io/axoniq/axonserver:2025.2.7`).

```bash
JAVA_HOME=<arm64 JDK 21+> ./mvnw -o test -pl integrationtests \
  -Dtest=WorkflowScalingPerfTest -Dsurefire.failIfNoSpecifiedTests=false \
  -Dperf.running=100,500,1000,2000,5000,10000,20000 \
  -Dperf.steps=0,10,50,100,250,500,1000,2000,5000 \
  -Dperf.completed=0,10000,50000,200000,500000 \
  -Dperf.restoreCeilingSeconds=900 -Dperf.out=results.csv
```

Sizes are comma lists. A restore that passes the ceiling records `-1` and the sweep continues.
Full run is about 50 minutes; seeding 500k finished workflows into Axon Server alone takes 12 minutes.

Regenerate the report from a CSV:

```bash
python3 report.py results.csv report.html '{"store":"...","engine":"...","processor":"...","jdk":"...","date":"...","harness":"...","segments":16}' template.html narrative.js
```

## Known state of this branch

Twelve pre-existing workflow integration tests on `poc/tla_dst` do not compile: they use the sync
`WorkflowHistoryRepository` API that the workflow manager backport made async. Move them aside or
add `.join()` before running anything in `integrationtests`. The perf test itself compiles.

Sweep 3 without snapshots was measured with a fresh token store per restart. Everything else carried
the token over.
