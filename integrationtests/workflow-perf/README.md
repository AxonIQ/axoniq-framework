# Workflow hibernation load test

Load test of workflow hibernation on real infrastructure only. The event store is Postgres through
`PostgresqlEventStorageEngine`, or Axon Server with a DCB context. The token store is always a `JpaTokenStore` on
PostgreSQL. Nothing a node stores lives in memory only. Only built with `-Pperf`.

Results and the report: [results/REPORT.md](results/REPORT.md).

## Workflows

5 production-shaped workflows in `LoadWorkflows`, an equal share each. Each one parks on its first wait right after it
starts:

1. **Order fulfilment**: waits up to 3 days for `PaymentReceived` by orderId, reserves stock with 3 retries, waits up
   to 7 days for `OrderShipped`.
2. **Subscription renewal**: sends a welcome, sleeps until `renewAt` (30 days), charges the card with 4 retries. Every
   10th first charge is declined.
3. **KYC review**: waits up to 2 days for `KycDecisionMade` by caseId, escalates on timeout, waits 5 more days.
4. **Loan application**: `allMatch` over `PayslipReceived`, `IdentityVerified` and `BankStatementReceived` by
   applicationId, 14 days each, then scores.
5. **Shipment tracking**: `anyMatch` over `ParcelDelivered` and `ParcelLost` by shipmentId, 10 days, then notifies.

## Phases

`HibernationLoadTest` runs one size of one arm per JVM:

1. **Start**: publishes N start events from 16 threads. Time until each workflow parked on its first wait, then until
   each hibernated. Heap per workflow after a full GC.
2. **Wake**: publishes `PaymentReceived` for up to 500 orders, 20 per second. Latency is publish to the first attempt
   of `reserveStock`.
3. **Deadlines**: 50 subscriptions whose sleeps end 200 ms apart (accuracy), then N/10 (100 to 10k) whose sleeps all
   end at the same instant (storm). Lateness is the deadline to the first `chargeCard` attempt.
4. **Restart**: stops the node and starts a new one on the same store and token store. Time until every live workflow
   is restored, then hibernated again, and the peak heap meanwhile.

Read load is counted by decorating the storage engine and the snapshot store, and on Postgres also from
`pg_stat_database`. A phase that passes its ceiling (`-Dperf.ceilingSeconds`, 900 by default) records `-1` and ends
the run: that is a wall.

The workflow module runs without history (`withoutHistory()`), because the default in-memory history would hold every
workflow and hide what hibernation saves. 16 segments, 16 worker threads, `-Xmx6g`, JDK 25.

## Running

The images must be local; `run.sh` never pulls. It recreates the containers before each size.

```bash
integrationtests/workflow-perf/run.sh axonserver true true 100,1000,10000 -Dperf.index=jpa
```

Arguments: store (`postgres` or `axonserver`), hibernation, snapshots, sizes, then extra `-D` flags:

| Flag | Meaning |
|---|---|
| `-Dperf.index=jpa` | Runs with the JPA workflow index. `false`, the default, runs without an index. |
| `-Dperf.startRate=200` | Publishes 200 start events per second. `0`, the default, publishes 1 burst. |
| `-Dperf.crash=true` | The node loses its database before the restart, so it releases no claim. |

`TEST=scalesFromTwoNodesToThreeToOne` runs the scaling test instead: 2 nodes, then 3, then 1.

Rows append to
`results/results.csv`, so a rerun of the same arm and size adds rows that supersede the earlier ones; `summarize.py`
keeps the last value. Logs go to `results/logs/`, which is not committed; `results/evidence/` keeps the parts the
issues refer to.

```bash
python3 integrationtests/workflow-perf/summarize.py integrationtests/workflow-perf/results/results.csv
```

`WaitAssociationIndexMatchCostTest` is a reproducer for the routing cost of the wait association index; it needs no
store:

```bash
./mvnw -o test -Pperf -pl integrationtests/workflow-perf -Dtest=WaitAssociationIndexMatchCostTest -Dsurefire.failIfNoSpecifiedTests=false
```

The Postgres engine logs `Waiting for license` once a minute without a license; `run.sh` leaves those lines out of the
error count.

## Fault campaign and upgrade test

`WorkflowIndexFaultCampaignTest` runs a seeded sequence of operations on 2 to 3 nodes: cases open, get approved and
close, while nodes crash, restart, join and leave, segments move, split and merge, and the whole cluster goes down.
Every case must run each effect exactly once, and the last node must know exactly the open cases. The arm sets what
the nodes run with: `noindex`, `indexed`, `owntx`, `enable`, `toggled` or `readfaults`.

```bash
integrationtests/workflow-perf/campaign.sh noindex,indexed 1,2,3
```

`upgrade.sh` runs the upgrade test. The released 5.4.1 engine, in `upgrade-5.4.1`, leaves live workflows in every
stage. Then `WorkflowIndexUpgradeTest` takes them over with the workflow index. `-Dupgrade.index=false` runs the same
upgrade without the index.

```bash
integrationtests/workflow-perf/upgrade.sh graceful
```

Both scripts start a fresh PostgreSQL on port 55450 and a fresh Axon Server on port 8125, and write to `results/`.
`PG_PORT`, `AS_HTTP`, `AS_GRPC` and `CONTAINER_PREFIX` give a run ports and container names of its own, so two runs
can share the machine.

