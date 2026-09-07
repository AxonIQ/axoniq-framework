package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.Backend;
import io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.RejectionLog;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext.equalsTo;
import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.eventsNamed;
import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.startNode;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * S4 of the DCB append-conditions hunt: restore-seed soundness (CL-7, SUS-4). Instances run to mid-flight (parked on
 * waitForEvent), the node shuts down, a fresh node over the same store restores them, and their first append after
 * restore must be accepted: every instance completes with zero rejection warnings. Includes the mixed-position arm:
 * one instance parks early while another appends later events before the restart.
 */
class S4RestoreSeedTest {

    @Test
    @Timeout(600)
    void inMemory() {
        for (int cycle = 1; cycle <= 10; cycle++) {
            runCycle(Backend.IN_MEMORY, cycle);
        }
        mixedPositions(Backend.IN_MEMORY);
    }

    @Test
    @Tag("docker")
    @Timeout(900)
    void postgres() {
        for (int cycle = 1; cycle <= 3; cycle++) {
            runCycle(Backend.POSTGRES, cycle);
        }
        mixedPositions(Backend.POSTGRES);
    }

    @Test
    @Tag("docker")
    @Timeout(900)
    void axonServer() {
        for (int cycle = 1; cycle <= 3; cycle++) {
            runCycle(Backend.AXON_SERVER, cycle);
        }
        mixedPositions(Backend.AXON_SERVER);
    }

    /** 5 instances park on waitForEvent, node restarts over the same store, releases arrive, all must complete. */
    private void runCycle(Backend backend, int cycle) {
        var store = DcbFencingBackends.freshStore(backend);
        var workflow = new ParkingWorkflow();
        List<String> ids = List.of("s4-" + backend + "-c" + cycle + "-i1",
                                   "s4-" + backend + "-c" + cycle + "-i2",
                                   "s4-" + backend + "-c" + cycle + "-i3",
                                   "s4-" + backend + "-c" + cycle + "-i4",
                                   "s4-" + backend + "-c" + cycle + "-i5");
        try (var rejections = new RejectionLog()) {
            try (var node = startNode(store, "s4-restore", workflow)) {
                ids.forEach(id -> node.publish(new StartParking(id, 0)));
                await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    for (String id : ids) {
                        assertThat(eventsNamed(store, id, "WaitReleaseStarted")).hasSize(1);
                    }
                    assertThat(node.runningWorkflowIds()).containsAll(ids);
                });
            }

            // Fresh node over the same store: segment claim, loadRunningWorkflows, marker seeding.
            try (var node = startNode(store, "s4-restore", new ParkingWorkflow())) {
                await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                        assertThat(node.runningWorkflowIds()).containsAll(ids));

                ids.forEach(id -> node.publish(new ReleaseParked(id)));

                await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    for (String id : ids) {
                        assertThat(eventsNamed(store, id, "ParkingWorkflowCompleted"))
                                .as("cycle %d: %s must complete after restore", cycle, id)
                                .hasSize(1);
                    }
                });
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(node.runningWorkflowIds()).isEmpty());
            }

            for (String id : ids) {
                assertThat(rejections.rejectionsFor(id))
                        .as("cycle %d: restored instance %s must not be (self-)rejected", cycle, id)
                        .isEmpty();
                assertThat(eventsNamed(store, id, "AfterReleaseCompleted")).hasSize(1);
            }
        }
    }

    /** Instance A parks EARLY, instance B appends 6 later steps first; after restart A must still complete. */
    private void mixedPositions(Backend backend) {
        var store = DcbFencingBackends.freshStore(backend);
        String early = "s4-mixed-" + backend + "-early";
        String busy = "s4-mixed-" + backend + "-busy";
        try (var rejections = new RejectionLog()) {
            try (var node = startNode(store, "s4-restore", new ParkingWorkflow())) {
                node.publish(new StartParking(early, 0));
                await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                        assertThat(eventsNamed(store, early, "WaitReleaseStarted")).hasSize(1));

                // B starts after A already parked, so all of B's step events sit past A's last own event.
                node.publish(new StartParking(busy, 6));
                await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    assertThat(eventsNamed(store, busy, "Busy6Completed")).hasSize(1);
                    assertThat(eventsNamed(store, busy, "WaitReleaseStarted")).hasSize(1);
                });
            }

            try (var node = startNode(store, "s4-restore", new ParkingWorkflow())) {
                await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                        assertThat(node.runningWorkflowIds()).contains(early, busy));

                node.publish(new ReleaseParked(early));
                node.publish(new ReleaseParked(busy));

                await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    assertThat(eventsNamed(store, early, "ParkingWorkflowCompleted"))
                            .as("early-parked instance must complete after restart").hasSize(1);
                    assertThat(eventsNamed(store, busy, "ParkingWorkflowCompleted")).hasSize(1);
                });
            }

            assertThat(rejections.rejectionsFor(early)).isEmpty();
            assertThat(rejections.rejectionsFor(busy)).isEmpty();
        }
    }

    public record StartParking(String id, int busySteps) {

    }

    public record ReleaseParked(String id) {

    }

    public static final class ParkingWorkflow {

        @Workflow(
                workflowName = "ParkingWorkflow",
                idProperty = "id",
                startOnEventClass = StartParking.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            int busySteps = ((Number) ctx.workflowPayload().getOrDefault("busySteps", 0)).intValue();
            for (int i = 1; i <= busySteps; i++) {
                int n = i;
                ctx.awaitExecute("busy" + n, Map.of(), (c, p) -> Map.of("busy" + n, n));
            }
            ctx.awaitEvent(
                    "waitRelease",
                    ReleaseParked.class,
                    associate(payloadProperty("id"), equalsTo(ctx.workflowId())),
                    step -> step.timeout(Duration.ofMinutes(5))
            );
            ctx.awaitExecute("afterRelease", Map.of(), (c, p) -> Map.of("done", true));
        }
    }
}
