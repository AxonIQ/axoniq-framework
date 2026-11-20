# Workflow load runner

A runnable example that starts many instances of a workflow, and measures how long the application takes to start up
while workflow instances are in progress. Use it to put load on Axon Server, and to compare restarts with the
workflow index kept in a database against restarts without an index.

## The workflow

`LoadWorkflow` has four steps:

1. `prepare` — an execute step that records when the instance was prepared.
2. `coolDown` — a two-second sleep, so every instance passes through a timer.
3. `awaitApproval` — waits for a `LoadApproved` event of this instance, with a long timeout (`--wait`, one hour by
   default). Most instances are still waiting here when you stop the application.
4. `finish` — an execute step that records whether the instance was approved or timed out.

A `LoadStarted` event starts an instance. Its `loadId`, `load-<run id>-<n>`, is the workflow id. The runner marks a
share of the start events (`--complete`, 10% by default) with `complete=true`. Once a second, it asks the
`WorkflowManager` for the marked instances that wait in `awaitApproval`, and publishes a `LoadApproved` event for each.

## Setup

- **Event store**: Axon Server, through the Axon Server connector.
- **Token store**: a `JpaTokenStore`, so the workflow processor resumes from its position after a restart.
- **Workflow index**: a `JpaWorkflowIndex` (`--index=jpa`, the runner's default), no index (`--index=none`, the
  engine's default), or an `InMemoryWorkflowIndex` (`--index=memory`). Without an index, every segment claim reads the
  list of all running workflows from the event store. The in-memory index is empty after a restart, so after a restart
  it behaves like no index for the first claims. A run with `none` or `memory` empties the tables of the JPA index,
  because it advances the processor's tokens without writing that index; the next run with `jpa` then initializes the
  index again from the event store.
- **Database**: an embedded HSQLDB file database in `--data-dir` (default `./target/load-runner-data`), holding the
  token store and the workflow index.
- **Workflow processor**: `--segments` segments (default 16) on first start, with as many threads, a batch size of 100,
  a checkpoint interval of one second, and a token claim interval of 500 milliseconds (see below). Workflow history is disabled: its projector replays every workflow event
  at each start, which would dominate the startup this runner measures.

`LoadRunnerSetup` holds all of this configuration.

## Prerequisites

- Java 21 or later. Java 24 or later is recommended for workflows under load, see the reference guide on performance
  tuning.
- A running Axon Server with a DCB context, which the workflow engine requires. For example, in standalone mode:

  ```shell
  docker run -d --name axonserver -p 8024:8024 -p 8124:8124 \
      -e AXONIQ_AXONSERVER_STANDALONE_DCB=true axoniq/axonserver
  ```

## Build

From the root of the repository:

```shell
./mvnw -Pexamples -pl examples/workflow-load-runner -am package -DskipTests
```

This builds the runnable jar `examples/workflow-load-runner/target/workflow-load-runner.jar`.

## Run

```shell
java -jar examples/workflow-load-runner/target/workflow-load-runner.jar <instances> [options]
```

```
  <instances>             number of workflow instances to start; 0 only starts up and measures

Options:
  --wait=<duration>       how long an instance waits for its approval event before it times
                          out, as an ISO-8601 duration (default: PT1H)
  --complete=<share>      share of the started instances the runner approves, 0..1 (default: 0.1)
  --segments=<n>          segments (and threads) of the workflow processor on first start (default: 16)
  --index=jpa|memory|none where to keep the workflow index: in the database, in memory, or
                          nowhere, so every claim reads the running workflows (default: jpa)
  --axon-server=<address> Axon Server address (default: localhost:8124)
  --data-dir=<dir>        directory of the token store and workflow index database
                          (default: ./target/load-runner-data)
  --in-memory-events      use an in-memory event store instead of Axon Server; nothing survives
                          a restart, for trying the runner without Axon Server
  --exit                  stop once the instances are started and wait for their approval,
                          instead of running until Ctrl+C
  --help                  print this text
```

Without `--exit`, the runner keeps running, and logs its progress every five seconds, until you stop it with Ctrl+C.
Stopping it cleanly stores the processor's tokens and releases its segments. After a hard kill, the next start waits
for the claims of the killed process to time out (ten seconds) before it can claim the segments.

`--segments` only applies on first start, when the token store has no tokens yet. Remove the data directory to start
over with another segment count, and use a fresh Axon Server as well: a new token store starts at the head of the
event stream, so instances of earlier runs that are still waiting would not be restored.

## Scenario: restore time with and without the JPA index

```shell
JAR=examples/workflow-load-runner/target/workflow-load-runner.jar

# 1. Start 10000 instances. They stay in progress for an hour, waiting for their approval.
java -jar $JAR 10000 --exit

# 2. Restart without starting new instances, restoring the instances in progress from the JPA index.
java -jar $JAR 0 --exit

# 3. Restart without an index: every segment claim reads the list of all running workflows instead.
java -jar $JAR 0 --exit --index=none

# 4. Restart with the JPA index again. Step 3 emptied it, so this run initializes it from the event store,
#    and only the run after this one restores from the index again.
java -jar $JAR 0 --exit
java -jar $JAR 0 --exit
```

Compare the `main -> workflow processor ready` lines of steps 2 and 3. Both restore the same instances in progress.

## What the numbers mean

At startup the runner prints:

```
=== Startup ===
JVM start -> main:                   ... ms
main -> database opened:             ... ms
main -> configuration started:       ... ms
main -> all segments claimed:        ... ms
main -> workflow processor ready:    ... ms
workflow instances in progress:      ...
```

- **JVM start -> main**: JVM startup and class loading before `main` runs.
- **main -> database opened**: until the `EntityManagerFactory` of the HSQLDB database is open.
- **main -> configuration started**: until `WorkflowConfigurer.start()` returned. This connects to Axon Server and
  starts the workflow processor, which claims its segments in the background.
- **main -> all segments claimed**: until every segment of the token store shows up in the processor's
  `processingStatus()`. A segment shows up when the processor starts restoring its workflow instances. The processor
  restores its newly claimed segments one after the other, so at this point only the instances of the last segment
  may still be restoring.
- **main -> workflow processor ready**: until the workflow processor holds every segment of its token store, and is
  caught up with the event store in each of them, according to `StreamingEventProcessor.processingStatus()`. The
  processor restores the running workflow instances of a segment when it claims the segment, before it processes any
  event of it, so this includes restoring every instance in progress. Without new events, the processor only marks a
  segment caught up when its coordinator checks again, at most the token claim interval later. The runner lowers that
  interval from 5 seconds to 500 milliseconds, so this number is accurate to about half a second.
- **workflow instances in progress**: the number of workflow instances with status `STARTED` on this node at that
  moment, according to the `WorkflowManager`.

After starting the instances it prints:

```
=== Starting 10000 instances ===
start events published in:           ... ms (... events/s)
instances started in:                ... ms (... instances/s)
workflow instances in progress:      ...
```

- **start events published in**: from publishing the first start event until Axon Server acknowledged the last one.
  The runner publishes batches of 100 events, at most 16 batches at a time.
- **instances started in**: from publishing the first start event until every instance of this run reached the status
  `STARTED`, counted by a workflow status change listener.

The progress line, every five seconds, shows the start events published, the instances of this run that started, the
instances in progress on this node, the approvals published, the instances that completed or failed since this
process started, and how many segments the processor holds and how many of those are caught up.

## Without Axon Server

`--in-memory-events` uses an in-memory event store and an in-memory database, so the runner works without Axon
Server. Nothing survives a restart. The smoke test, `WorkflowLoadRunnerTest`, runs the runner this way.
