package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.Backend;
import io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.RejectionLog;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.payload.CombineGlobalAndLocalPayloadReducer;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.appendDirectly;
import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.eventNames;
import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.eventsNamed;
import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.eventsWithTags;
import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.startNode;
import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.workflowTag;
import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.TAG_VALUE_EVENT_TYPE_LIFECYCLE;
import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.TAG_WORKFLOW_EVENT_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * S3 of the DCB append-conditions hunt: the ConcurrentWriterFencingTest topology (two engines, one store, separate
 * token stores) per backend, several rounds with a fresh store each. Counts how many rounds the loser's rejection
 * warning appeared (the race provably landed). Also re-runs the id-reuse (ORIGIN anchor) check per backend.
 */
class S3TwoNodeFencingTest {

    private static final Logger logger = LoggerFactory.getLogger(S3TwoNodeFencingTest.class);

    private static final MessageType DEFINITION_ID =
            new MessageType(new QualifiedName("RacedWorkflow"), MessageType.DEFAULT_VERSION);

    @Test
    @Timeout(600)
    void inMemory() {
        runRounds(Backend.IN_MEMORY, 10);
        idReuse(Backend.IN_MEMORY);
    }

    @Test
    @Tag("docker")
    @Timeout(900)
    void axonServer() {
        runRounds(Backend.AXON_SERVER, 5);
        idReuse(Backend.AXON_SERVER);
    }

    @Test
    @Tag("docker")
    @Timeout(900)
    void postgres() {
        runRounds(Backend.POSTGRES, 5);
        idReuse(Backend.POSTGRES);
    }

    private void runRounds(Backend backend, int rounds) {
        int racesLanded = 0;
        int bothSpawned = 0;
        for (int r = 1; r <= rounds; r++) {
            var store = DcbFencingBackends.freshStore(backend);
            var bodyRuns = new CopyOnWriteArrayList<String>();
            var stepActionRuns = new AtomicInteger();
            String id = "s3-" + backend + "-r" + r;
            var workflow = new RacedWorkflow(bodyRuns, stepActionRuns);
            try (var rejections = new RejectionLog();
                 var nodeA = startNode(store, "s3-two-node", workflow);
                 var nodeB = startNode(store, "s3-two-node", workflow)) {

                nodeA.publish(new StartRaced(id));

                await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    assertThat(bodyRuns).isNotEmpty();
                    assertThat(eventsNamed(store, id, "RacedWorkflowCompleted")).hasSize(1);
                });
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(nodeA.runningWorkflowIds()).isEmpty());
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(nodeB.runningWorkflowIds()).isEmpty());

                // Safety oracle, every round: one start, no duplicate facts, action ran once.
                assertThat(eventsNamed(store, id, "RacedWorkflowStarted")).hasSize(1);
                assertThat(eventNames(store, id)).doesNotHaveDuplicates();
                assertThat(stepActionRuns).hasValue(1);

                long spawns = bodyRuns.stream().filter(id::equals).count();
                boolean rejected = !rejections.rejectionsFor(id).isEmpty();
                if (spawns >= 2) {
                    bothSpawned++;
                }
                if (rejected) {
                    racesLanded++;
                }
                logger.info("S3 {} round {}: spawns={} rejectionWarnings={}",
                                  backend, r, spawns, rejections.rejectionsFor(id).size());
            }
        }
        logger.info("S3 {} SUMMARY: rounds={} bothSpawned={} racesLanded(rejection observed)={}",
                          backend, rounds, bothSpawned, racesLanded);
        assertThat(racesLanded)
                .as("%s: at least one round must show the loser's rejection, otherwise the run is inconclusive",
                    backend)
                .isGreaterThan(0);
    }

    private void idReuse(Backend backend) {
        var store = DcbFencingBackends.freshStore(backend);
        var bodyRuns = new CopyOnWriteArrayList<String>();
        var stepActionRuns = new AtomicInteger();
        String id = "s3-reuse-" + backend;
        var converter = DcbFencingBackends.converter();

        appendDirectly(store, new GenericEventMessage(
                new MessageType("RacedWorkflowStarted"),
                Map.of("id", id),
                MetadataUtils.create(id, WorkflowStatus.STARTED, DEFINITION_ID)
                             .and(MetadataUtils.METADATA_KEY_MODIFY_PAYLOAD, CombineGlobalAndLocalPayloadReducer.NAME)
        ).withConverter(converter));
        appendDirectly(store, new GenericEventMessage(
                new MessageType("RacedWorkflowCompleted"),
                Map.of(),
                MetadataUtils.create(id, WorkflowStatus.COMPLETED, DEFINITION_ID)
        ).withConverter(converter));

        try (var node = startNode(store, "s3-id-reuse", new RacedWorkflow(bodyRuns, stepActionRuns))) {
            node.publish(new StartRaced(id));
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(node.runningWorkflowIds()).isEmpty());

            assertThat(eventsWithTags(store, workflowTag(id),
                                      org.axonframework.messaging.eventstreaming.Tag.of(
                                              TAG_WORKFLOW_EVENT_TYPE, TAG_VALUE_EVENT_TYPE_LIFECYCLE)))
                    .as("%s: seeded terminated history must stay untouched", backend)
                    .hasSize(2);
            assertThat(bodyRuns).as("%s: body of a reused id must not run", backend).isEmpty();
            assertThat(stepActionRuns).hasValue(0);
        }
    }

    public record StartRaced(String id) {

    }

    public static final class RacedWorkflow {

        private final List<String> bodyRuns;
        private final AtomicInteger stepActionRuns;

        public RacedWorkflow(List<String> bodyRuns, AtomicInteger stepActionRuns) {
            this.bodyRuns = bodyRuns;
            this.stepActionRuns = stepActionRuns;
        }

        @Workflow(
                workflowName = "RacedWorkflow",
                idProperty = "id",
                startOnEventClass = StartRaced.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            bodyRuns.add(ctx.workflowId());
            ctx.awaitExecute("ship", Boolean.class, () -> {
                stepActionRuns.incrementAndGet();
                return true;
            });
        }
    }
}
