package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple.SimpleContext;
import io.axoniq.workflow.dsl.simple.SimpleDefinition;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.Coordinator;
import io.axoniq.workflow.runtime.engine.StateManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.workflow.dsl.simple.Payload.payload;
import static io.axoniq.workflow.runtime.DelayedPublisher.Schedule.ofMillis;
import static io.axoniq.workflow.runtime.context.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests that verify the subscription-based event notification works correctly
 * with multiple parallel workflows, waitForEvent, and execute with timeout.
 *
 * This test validates:
 * 1. Two workflows can running in parallel
 * 2. waitForEvent with subscription-based notification
 * 3. execute with timeout using ScheduledExecutor
 * 4. Proper event routing to the correct workflow instance
 */
class ParallelWorkflowSubscriptionTest {

    private static final Logger logger = LoggerFactory.getLogger(ParallelWorkflowSubscriptionTest.class);

    private StateManager stateManager;
    private Coordinator coordinator;
    private DelayedPublisher delayedPublisher;

    // Track how many times services are called to verify correct execution
    private static final AtomicInteger externalApiCalls = new AtomicInteger(0);

    // ============== DOMAIN EVENTS ==============

    record OrderCreatedEvent(String orderId, String customerId, int amount, long externalApiDelayMs) {}
    record PaymentReceivedEvent(String orderId, String paymentId, int amount) {}
    record ExternalValidationResult(String orderId, boolean valid, String message) {}

    // ============== MOCK SERVICES ==============

    static class ExternalApiService {
        static ExternalValidationResult callExternalApi(String orderId, long delayMs) {
            int callNum = externalApiCalls.incrementAndGet();
            logger.info("[ExternalApi] Call #{} - Validating order {} (will take {}ms)", callNum, orderId, delayMs);
            sleep(delayMs);
            logger.info("[ExternalApi] Validation complete for order {}", orderId);
            return new ExternalValidationResult(orderId, true, "Validated successfully");
        }
    }

    static class NotificationService {
        static void notifyCustomer(String customerId, String message) {
            logger.info("[NotificationService] Notifying {}: {}", customerId, message);
            sleep(50);
        }
    }

    // ============== WORKFLOW DEFINITION ==============

    /**
     * Workflow that:
     * 1. Executes an initial step
     * 2. Waits for a PaymentReceivedEvent (subscription-based, no polling)
     * 3. Calls an external API with timeout (tests timeout mechanism)
     * 4. Sends notification
     */
    static class OrderProcessingWorkflow extends SimpleDefinition.Type {

        private static final Duration EXTERNAL_API_TIMEOUT = Duration.ofMillis(500);

        @Override
        public String workflowId(Map<String, Object> trigger) {
            return "order-" + trigger.get("orderId");
        }

        @Override
        public void execute(SimpleContext context) {
            var triggerData = payload(context);
            String orderId = triggerData.get("orderId");
            String customerId = triggerData.get("customerId");
            int amount = triggerData.get("amount");
            long externalApiDelay = ((Number) triggerData.get("externalApiDelayMs")).longValue();

            logger.info("========== [{}] Starting Order Processing ==========", orderId);

            // Step 1: Initial processing
            context.execute("initializeOrder",
                payload("orderId", orderId),
                Void.class,
                p -> {
                    logger.info("[{}] Initializing order", orderId);
                    sleep(100);
                    return null;
                });

            // Step 2: WAIT FOR EVENT - PaymentReceivedEvent
            // This tests subscription-based waiting (no polling!)
            logger.info("[{}] >>> Waiting for payment event (subscription-based)...", orderId);
            PaymentReceivedEvent payment = context.waitForEvent(
                "awaitPayment",
                PaymentReceivedEvent.class,
                event -> event.orderId().equals(orderId),  // Match by orderId
                Duration.ofSeconds(30)
            );
            logger.info("[{}] >>> Received payment: {}", orderId, payment.paymentId());

            // Step 3: EXECUTE WITH TIMEOUT - Call external API
            // This tests timeout mechanism with ScheduledExecutor (no polling!)
            logger.info("[{}] >>> Calling external API with {}ms timeout...", orderId, EXTERNAL_API_TIMEOUT.toMillis());
            ExternalValidationResult validation;
            try {
                validation = callExternalApiWithTimeout(context, orderId, externalApiDelay);
                logger.info("[{}] >>> External API returned: {}", orderId, validation.message());
            } catch (CompletionException e) {
                if (e.getCause() instanceof TimeoutException) {
                    logger.warn("[{}] --- External API timed out! Running compensation ---", orderId);

                    context.execute("notifyTimeout",
                        payload("customerId", customerId, "orderId", orderId),
                        Void.class,
                        p -> {
                            NotificationService.notifyCustomer(
                                p.get("customerId"),
                                "Order " + p.get("orderId") + " processing delayed - external validation pending"
                            );
                            return null;
                        });

                    logger.info("[{}] --- Compensation complete ---", orderId);
                    return;
                }
                throw e;
            }

            // Step 4: Final notification
            context.execute("sendConfirmation",
                payload("customerId", customerId, "orderId", orderId, "paymentId", payment.paymentId()),
                Void.class,
                p -> {
                    NotificationService.notifyCustomer(
                        p.get("customerId"),
                        "Order " + p.get("orderId") + " confirmed with payment " + p.get("paymentId")
                    );
                    return null;
                });

            logger.info("========== [{}] Order Processing Complete ==========", orderId);
        }

        private ExternalValidationResult callExternalApiWithTimeout(SimpleContext context, String orderId, long delayMs) {
            String resultKey = "__externalValidation";

            var result = context.execute(
                "callExternalApi",
                payload("orderId", orderId, "delayMs", delayMs).getValues(),
                p -> {
                    var payloadObj = payload(p);
                    ExternalValidationResult apiResult = ExternalApiService.callExternalApi(
                        payloadObj.get("orderId"),
                        payloadObj.get("delayMs")
                    );
                    return payload(resultKey, apiResult).getValues();
                },
                PayloadReducer.local(),
                PayloadReducer.all(),
                EXTERNAL_API_TIMEOUT,
                defaults()
            ).join();

            return (ExternalValidationResult) result.get(resultKey);
        }
    }

    // ============== HELPER ==============

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ============== TESTS ==============

    @BeforeEach
    void setUp() {
        stateManager = new StateManager();
        coordinator = new Coordinator(stateManager);
        delayedPublisher = new DelayedPublisher(stateManager);
        externalApiCalls.set(0);
    }

    @AfterEach
    void tearDown() {
        stateManager.printPayloads();
        coordinator.stop();
    }

    @Test
    void shouldRunTwoWorkflowsInParallelWithWaitForEventAndTimeout() {
        // Given - two orders that will be processed in parallel
        coordinator.register(OrderProcessingWorkflow.class, OrderCreatedEvent.class);

        // Schedule events:
        // - Two OrderCreatedEvent at nearly the same time (parallel workflows)
        // - Payment events arrive later (tests subscription-based waiting)
        // - Both external API calls should complete within timeout
        delayedPublisher.addSchedules(List.of(
            // Start both workflows nearly simultaneously
            ofMillis(100, new OrderCreatedEvent("ORDER-A", "CUST-1", 100, 200)),  // API takes 200ms < 500ms timeout
            ofMillis(150, new OrderCreatedEvent("ORDER-B", "CUST-2", 200, 300)),  // API takes 300ms < 500ms timeout

            // Payment events arrive later - ORDER-B payment arrives first!
            ofMillis(2000, new PaymentReceivedEvent("ORDER-B", "PAY-B-123", 200)),
            ofMillis(2500, new PaymentReceivedEvent("ORDER-A", "PAY-A-456", 100))
        ));

        // When
        coordinator.start();
        delayedPublisher.start();

        // Then - both workflows should complete successfully
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(coordinator.getHistory()).hasSize(2);
            assertThat(coordinator.getRunning()).isEmpty();
        });

        // Verify both workflows completed
        List<WorkflowContext> completed = coordinator.getHistory();

        // Find each workflow by ID
        WorkflowContext orderA = completed.stream()
            .filter(c -> c.getWorkflowId().equals("order-ORDER-A"))
            .findFirst().orElseThrow();
        WorkflowContext orderB = completed.stream()
            .filter(c -> c.getWorkflowId().equals("order-ORDER-B"))
            .findFirst().orElseThrow();

        // Verify ORDER-A steps
        assertThat(orderA.getStepHistory())
            .contains("initializeOrder", "awaitPayment", "callExternalApi", "sendConfirmation")
            .doesNotContain("notifyTimeout");

        // Verify ORDER-B steps
        assertThat(orderB.getStepHistory())
            .contains("initializeOrder", "awaitPayment", "callExternalApi", "sendConfirmation")
            .doesNotContain("notifyTimeout");

        // Verify external API was called exactly twice (once per workflow)
        assertThat(externalApiCalls.get()).isEqualTo(2);

        logger.info("=== ORDER-A Step History ===");
        orderA.getStepHistory().forEach(step -> logger.info("  - {}", step));
        logger.info("=== ORDER-B Step History ===");
        orderB.getStepHistory().forEach(step -> logger.info("  - {}", step));
    }

    @Test
    void shouldHandleTimeoutInOneWorkflowWhileOtherCompletes() {
        // Given - two orders: one will timeout, one will succeed
        coordinator.register(OrderProcessingWorkflow.class, OrderCreatedEvent.class);

        delayedPublisher.addSchedules(List.of(
            // Start both workflows
            ofMillis(100, new OrderCreatedEvent("ORDER-FAST", "CUST-1", 100, 200)),   // API takes 200ms < 500ms timeout
            ofMillis(150, new OrderCreatedEvent("ORDER-SLOW", "CUST-2", 200, 2000)),  // API takes 2000ms > 500ms timeout!

            // Both payments arrive
            ofMillis(1500, new PaymentReceivedEvent("ORDER-FAST", "PAY-FAST", 100)),
            ofMillis(1600, new PaymentReceivedEvent("ORDER-SLOW", "PAY-SLOW", 200))
        ));

        // When
        coordinator.start();
        delayedPublisher.start();

        // Then - both workflows should complete (one with timeout compensation)
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(coordinator.getHistory()).hasSize(2);
            assertThat(coordinator.getRunning()).isEmpty();
        });

        List<WorkflowContext> completed = coordinator.getHistory();

        WorkflowContext orderFast = completed.stream()
            .filter(c -> c.getWorkflowId().equals("order-ORDER-FAST"))
            .findFirst().orElseThrow();
        WorkflowContext orderSlow = completed.stream()
            .filter(c -> c.getWorkflowId().equals("order-ORDER-SLOW"))
            .findFirst().orElseThrow();

        // ORDER-FAST should complete successfully
        assertThat(orderFast.getStepHistory())
            .contains("initializeOrder", "awaitPayment", "callExternalApi", "sendConfirmation")
            .doesNotContain("notifyTimeout");

        // ORDER-SLOW should have timeout and compensation
        assertThat(orderSlow.getStepHistory())
            .contains("initializeOrder", "awaitPayment", "callExternalApi", "notifyTimeout")
            .doesNotContain("sendConfirmation");

        logger.info("=== ORDER-FAST Step History (success) ===");
        orderFast.getStepHistory().forEach(step -> logger.info("  - {}", step));
        logger.info("=== ORDER-SLOW Step History (timeout) ===");
        orderSlow.getStepHistory().forEach(step -> logger.info("  - {}", step));
    }

    @Test
    void shouldCorrectlyRouteEventsToMatchingWorkflows() {
        // Given - three workflows waiting for events, events arrive in scrambled order
        coordinator.register(OrderProcessingWorkflow.class, OrderCreatedEvent.class);

        delayedPublisher.addSchedules(List.of(
            // Start three workflows
            ofMillis(100, new OrderCreatedEvent("ORDER-1", "CUST-A", 100, 100)),
            ofMillis(100, new OrderCreatedEvent("ORDER-2", "CUST-B", 200, 100)),
            ofMillis(100, new OrderCreatedEvent("ORDER-3", "CUST-C", 300, 100)),

            // Payments arrive in reverse order (tests correct event routing)
            ofMillis(2000, new PaymentReceivedEvent("ORDER-3", "PAY-3", 300)),
            ofMillis(2100, new PaymentReceivedEvent("ORDER-1", "PAY-1", 100)),
            ofMillis(2200, new PaymentReceivedEvent("ORDER-2", "PAY-2", 200))
        ));

        // When
        coordinator.start();
        delayedPublisher.start();

        // Then - all three workflows should complete
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(coordinator.getHistory()).hasSize(3);
            assertThat(coordinator.getRunning()).isEmpty();
        });

        // Verify all completed successfully (correct event routing)
        coordinator.getHistory().forEach(ctx -> {
            assertThat(ctx.getStepHistory())
                .contains("initializeOrder", "awaitPayment", "callExternalApi", "sendConfirmation")
                .doesNotContain("notifyTimeout");
            logger.info("=== {} completed successfully ===", ctx.getWorkflowId());
        });

        // Verify external API was called exactly 3 times
        assertThat(externalApiCalls.get()).isEqualTo(3);
    }
}
