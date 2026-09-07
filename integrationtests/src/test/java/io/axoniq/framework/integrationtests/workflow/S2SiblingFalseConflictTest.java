package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.Backend;
import io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.RejectionLog;
import io.axoniq.framework.workflow.runtime.api.annotation.Workflow;
import io.axoniq.framework.workflow.runtime.api.execution.context.retry.BackoffStrategy;
import io.axoniq.framework.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.appendDirectly;
import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.eventNames;
import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.eventsNamed;
import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.startNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * S2 of the DCB append-conditions hunt: a single engine running one instance with 8 parallel steps, combinators, a
 * step cancel, and a retry loop must never reject its own sibling appends (CL-4/CL-5). Negative control per backend:
 * a foreign event tagged with the same workflowId, appended directly to the store mid-run, must get the instance's
 * next append rejected, proving the condition is enforced on that backend.
 */
class S2SiblingFalseConflictTest {

    private static final Duration LONG = Duration.ofMinutes(5);

    private static String cap(String name) {
        return Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }

    @Test
    @Timeout(240)
    void inMemory() {
        runRepeats(Backend.IN_MEMORY, 20);
        negativeControl(Backend.IN_MEMORY);
    }

    @Test
    @Tag("docker")
    @Timeout(600)
    void axonServer() {
        runRepeats(Backend.AXON_SERVER, 5);
        negativeControl(Backend.AXON_SERVER);
    }

    @Test
    @Tag("docker")
    @Timeout(600)
    void postgres() {
        runRepeats(Backend.POSTGRES, 5);
        negativeControl(Backend.POSTGRES);
    }

    private void runRepeats(Backend backend, int repeats) {
        var store = DcbFencingBackends.freshStore(backend);
        var workflow = new SiblingWorkflow();
        try (var rejections = new RejectionLog(); var node = startNode(store, "s2-siblings", workflow)) {
            for (int r = 1; r <= repeats; r++) {
                String id = "s2-" + backend + "-" + r;
                node.publish(new StartSibling(id));

                await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                        assertThat(eventsNamed(store, id, "SiblingWorkflowCompleted")).hasSize(1));
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertThat(node.runningWorkflowIds()).doesNotContain(id));

                var names = eventNames(store, id);
                // Zero rejections of our own appends and the execution was never interrupted (it completed).
                assertThat(rejections.rejectionsFor(id)).as("run %s: no sibling append rejected", id).isEmpty();
                // Every parallel step terminal exactly once.
                for (int i = 1; i <= 8; i++) {
                    String step = "par" + i;
                    assertThat(names.stream().filter(n -> n.equals(cap(step) + "Completed")).count())
                            .as("run %s: %s completed once", id, step).isEqualTo(1);
                }
                assertThat(names.stream().filter(n -> n.equals("DoomedCancelled")).count())
                        .as("run %s: cancelled step terminal once", id).isEqualTo(1);
                assertThat(names.stream().filter(n -> n.equals("RetryingCompleted")).count())
                        .as("run %s: retry step terminal once", id).isEqualTo(1);
                // The retry loop really retried.
                assertThat(names.stream().filter(n -> n.equals("RetryingRetrying")).count())
                        .as("run %s: RETRYING events present", id).isGreaterThanOrEqualTo(3);
                // Workflow terminal exactly once.
                assertThat(names.stream().filter(n -> n.equals("SiblingWorkflowCompleted")).count()).isEqualTo(1);
            }
        }
    }

    /**
     * Proves append conditions are enforced on this backend: the instance parks between step1 and step2, a foreign
     * event tagged with its workflowId lands directly in the store, and the instance's next append must be rejected.
     */
    private void negativeControl(Backend backend) {
        var store = DcbFencingBackends.freshStore(backend);
        var workflow = new GatedWorkflow();
        String id = "s2-nc-" + backend;
        try (var rejections = new RejectionLog(); var node = startNode(store, "s2-negative-control", workflow)) {
            node.publish(new StartGated(id));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(eventsNamed(store, id, "Step1Completed")).hasSize(1));

            // Foreign writer sneaks an event for the same instance into the store.
            appendDirectly(store, new GenericEventMessage(
                    new MessageType("ForeignIntruderEvent"),
                    Map.of("id", id),
                    MetadataUtils.create(id, WorkflowStatus.STARTED,
                                         new MessageType(new QualifiedName("GatedWorkflow"),
                                                         MessageType.DEFAULT_VERSION))
            ).withConverter(DcbFencingBackends.converter()));

            workflow.release(id);

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(rejections.rejectionsFor(id))
                            .as("negative control on %s: next append must be rejected", backend)
                            .isNotEmpty());
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(node.runningWorkflowIds()).doesNotContain(id));

            // The rejected writer recorded nothing after the intruder: no step2 terminal, no workflow terminal.
            assertThat(eventsNamed(store, id, "Step2Completed")).isEmpty();
            assertThat(eventsNamed(store, id, "GatedWorkflowCompleted")).isEmpty();
        }
    }

    public record StartSibling(String id) {

    }

    public record StartGated(String id) {

    }

    public static final class SiblingWorkflow {

        private final Map<String, AtomicInteger> attemptsById = new ConcurrentHashMap<>();

        @Workflow(
                workflowName = "SiblingWorkflow",
                idProperty = "id",
                startOnEventClass = StartSibling.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            List<WorkflowStepResult> parallel = new ArrayList<>();
            for (int i = 1; i <= 8; i++) {
                int n = i;
                parallel.add(ctx.execute("par" + n, (c, p) -> {
                    sleep(20 + n * 10L);
                    return Map.of("par" + n, n);
                }, step -> step.timeout(LONG)));
            }
            var doomed = ctx.execute("doomed", (c, p) -> {
                sleep(LONG.toMillis());
                return Map.of();
            }, step -> step.timeout(LONG));

            ctx.allMatch(WorkflowStepResult::success, parallel.get(0), parallel.get(1)).await();
            ctx.anyMatch(WorkflowStepResult::success, parallel.get(2), parallel.get(3)).await();

            var attempts = attemptsById.computeIfAbsent(ctx.workflowId(), k -> new AtomicInteger());
            ctx.awaitExecute("retrying", Map.of(), (c, p) -> {
                int attempt = attempts.incrementAndGet();
                if (attempt <= 3) {
                    throw new RuntimeException("deliberate failure, attempt " + attempt);
                }
                return Map.of("retried", attempt);
            }, step -> step.retryPolicy(
                    RetryPolicy.maxRetries(3).withBackoff(BackoffStrategy.fixed(Duration.ofMillis(50)))));

            doomed.cancel("no longer needed");
            for (var result : parallel) {
                result.await();
            }
        }

        private static void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static final class GatedWorkflow {

        private final Map<String, CountDownLatch> gates = new ConcurrentHashMap<>();

        void release(String id) {
            gates.computeIfAbsent(id, k -> new CountDownLatch(1)).countDown();
        }

        @Workflow(
                workflowName = "GatedWorkflow",
                idProperty = "id",
                startOnEventClass = StartGated.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            ctx.awaitExecute("step1", Map.of(), (c, p) -> Map.of("a", 1));
            try {
                gates.computeIfAbsent(ctx.workflowId(), k -> new CountDownLatch(1))
                     .await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            ctx.awaitExecute("step2", Map.of(), (c, p) -> Map.of("b", 2));
        }
    }
}
