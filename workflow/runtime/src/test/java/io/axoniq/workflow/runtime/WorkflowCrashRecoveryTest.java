package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple.SimpleContext;
import io.axoniq.workflow.dsl.simple.SimpleDefinition;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.Coordinator;
import io.axoniq.workflow.runtime.engine.StateManager;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Test that verifies workflow crash recovery via event sourcing.
 *
 * When a workflow crashes mid-execution
 * 1. A new Coordinator with the SAME StateManager (history) can recover the workflow
 * 2. Already-completed steps are NOT re-executed (ExecuteDelegate returns cached results)
 * 3. The workflow resumes from where it left off
 */
@Disabled("Requires Coordinator changes to properly stop polling threads")
class WorkflowCrashRecoveryTest {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowCrashRecoveryTest.class);

  // Shared state manager - simulates persistent event store
  private StateManager stateManager;
  private Coordinator coordinator1;
  private Coordinator coordinator2;

  // Execution tracking - survives across coordinator instances
  private static final Map<String, AtomicInteger> stepExecutionCounts = new ConcurrentHashMap<>();

  // Thread capture for aggressive kill
  private static volatile Thread workflowThread;

  // Latches for synchronization
  private static CountDownLatch stepCompletedLatch;  // Signals when chargePayment is done
  private static CountDownLatch readyToKillLatch;    // Workflow waits here before step 4
  private static volatile boolean shouldBlockBeforeStep4 = false;

  // ============== DOMAIN RECORDS ==============

  record Order(String orderId, String customerId, String productId, int quantity, BigDecimal totalAmount) {}
  record InventoryResult(String productId, int available, boolean inStock) {}
  record PaymentResult(String transactionId, BigDecimal amount, String status) {}
  record ShipmentResult(String trackingNumber, String status) {}

  // Trigger event
  record OrderReceivedEvent(String orderId, String customerId, String productId, int quantity, BigDecimal totalAmount) {}

  // ============== INSTRUMENTED SERVICES ==============

  static class InstrumentedInventoryService {
    static InventoryResult checkInventory(String productId, int quantity) {
      int count = stepExecutionCounts.computeIfAbsent("checkInventory", k -> new AtomicInteger(0)).incrementAndGet();
      logger.info("[InventoryService] checkInventory execution #{}", count);
      simulateWork(50);
      return new InventoryResult(productId, 100, true);
    }

    static void reserveStock(String productId, int quantity) {
      int count = stepExecutionCounts.computeIfAbsent("reserveStock", k -> new AtomicInteger(0)).incrementAndGet();
      logger.info("[InventoryService] reserveStock execution #{}", count);
      simulateWork(50);
    }
  }

  static class InstrumentedPaymentService {
    static PaymentResult chargePayment(String customerId, BigDecimal amount) {
      int count = stepExecutionCounts.computeIfAbsent("chargePayment", k -> new AtomicInteger(0)).incrementAndGet();
      logger.info("[PaymentService] chargePayment execution #{}", count);
      simulateWork(50);
      String transactionId = "TXN-" + UUID.randomUUID().toString().substring(0, 8);
      return new PaymentResult(transactionId, amount, "COMPLETED");
    }
  }

  static class InstrumentedShippingService {
    static ShipmentResult createShipment(String orderId, String productId, int quantity) {
      int count = stepExecutionCounts.computeIfAbsent("createShipment", k -> new AtomicInteger(0)).incrementAndGet();
      logger.info("[ShippingService] createShipment execution #{}", count);
      simulateWork(50);
      String trackingNumber = "TRACK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
      return new ShipmentResult(trackingNumber, "LABEL_CREATED");
    }
  }

  static class InstrumentedNotificationService {
    static void sendConfirmation(String customerId, String orderId, String trackingNumber, String transactionId) {
      int count = stepExecutionCounts.computeIfAbsent("sendConfirmation", k -> new AtomicInteger(0)).incrementAndGet();
      logger.info("[NotificationService] sendConfirmation execution #{}", count);
      simulateWork(50);
      logger.info("[NotificationService] Confirmation sent for order {} with tracking {}", orderId, trackingNumber);
    }
  }

  private static void simulateWork(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during work simulation", e);
    }
  }

  // ============== SEQUENTIAL WORKFLOW ==============

  /**
   * Simple 5-step sequential workflow:
   * 1. checkInventory
   * 2. reserveStock
   * 3. chargePayment
   * <-- CRASH POINT
   * 4. createShipment
   * 5. sendConfirmation
   */
  static class SequentialOrderWorkflow extends SimpleDefinition.Type {

    @Override
    public String workflowId(Map<String, Object> trigger) {
      return "order-" + trigger.getOrDefault("orderId", UUID.randomUUID().toString());
    }

    //onFailed() ->
    //onSucess() ->

    @Override
    public void execute(SimpleContext context) {
      // Capture thread reference for aggressive kill
      workflowThread = Thread.currentThread();

      Map<String, Object> triggerData = context.getPayload();
      Order order = new Order(
        (String) triggerData.get("orderId"),
        (String) triggerData.get("customerId"),
        (String) triggerData.get("productId"),
        (Integer) triggerData.get("quantity"),
        (BigDecimal) triggerData.get("totalAmount")
      );

    //  if()  context.fail("failed because...");
    //  else  context.sucess(Optional payload);
    // context.timeout() and context.cancel() <--internal


      //nested-> you cant block on workflow


      logger.info("========== Starting Sequential Order Workflow: {} (thread: {}) ==========",
        order.orderId(), Thread.currentThread().getName());

      // Step 1: Check Inventory
      InventoryResult inventory = context.execute("checkInventory",
        Map.of("productId", order.productId(), "quantity", order.quantity()),
        InventoryResult.class,
        payload -> InstrumentedInventoryService.checkInventory(
          (String) payload.get("productId"),
          (Integer) payload.get("quantity")
        ));

      if (!inventory.inStock()) {
        logger.warn("Out of stock - workflow ending");
        return;
      }

      // Step 2: Reserve Stock
      context.execute("reserveStock",
        Map.of("productId", order.productId(), "quantity", order.quantity()),
        Void.class,
        payload -> {
          InstrumentedInventoryService.reserveStock(
            (String) payload.get("productId"),
            (Integer) payload.get("quantity")
          );
          return null;
        });

      // Step 3: Charge Payment
      PaymentResult payment = context.execute("chargePayment",
        Map.of("customerId", order.customerId(), "amount", order.totalAmount()),
        PaymentResult.class,
        payload -> InstrumentedPaymentService.chargePayment(
          (String) payload.get("customerId"),
          (BigDecimal) payload.get("amount")
        ));

      // *** CRASH POINT *** - signal completion and wait to be killed
      if (shouldBlockBeforeStep4) {
        logger.info(">>>Waiting to be killed... :(");
        stepCompletedLatch.countDown();  // Signal that step 3 is done
        try {
          // Block here - test will interrupt this thread
          readyToKillLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          logger.error("!!! CRASH OCCURRED !!!");
          Thread.currentThread().interrupt();
          throw new RuntimeException("Something killed the machine", e);
        }
      }

      // Step 4: Create Shipment
      ShipmentResult shipment = context.execute("createShipment",
        Map.of("orderId", order.orderId(), "productId", order.productId(), "quantity", order.quantity()),
        ShipmentResult.class,
        payload -> InstrumentedShippingService.createShipment(
          (String) payload.get("orderId"),
          (String) payload.get("productId"),
          (Integer) payload.get("quantity")
        ));

      // Step 5: Send Confirmation
      context.execute("sendConfirmation",
        Map.of("customerId", order.customerId(), "orderId", order.orderId(),
               "trackingNumber", shipment.trackingNumber(), "transactionId", payment.transactionId()),
        Void.class,
        payload -> {
          InstrumentedNotificationService.sendConfirmation(
            (String) payload.get("customerId"),
            (String) payload.get("orderId"),
            (String) payload.get("trackingNumber"),
            (String) payload.get("transactionId")
          );
          return null;
        });

      logger.info("==========  Order Workflow Complete: {} ==========", order.orderId());
    }
  }

  // ============== TEST SETUP ==============

  @BeforeEach
  void setUp() {
    // Reset static state
    stepExecutionCounts.clear();
    workflowThread = null;
    shouldBlockBeforeStep4 = false;
    stepCompletedLatch = new CountDownLatch(1);
    readyToKillLatch = new CountDownLatch(1);

    // Create shared state manager (simulates persistent event store)
    stateManager = new StateManager();
  }

  @AfterEach
  void tearDown() {
    if (coordinator1 != null) {
      try {
        coordinator1.stop();
      } catch (Exception ignored) {}
    }
    if (coordinator2 != null) {
      try {
        coordinator2.stop();
      } catch (Exception ignored) {}
    }
    stateManager.printPayloads();
  }

  // ============== TESTS ==============

  @Test
  void shouldRecoverFromCrashAndContinueFromLastCompletedStep() throws Exception {
    String orderId = "ORD-CRASH-001";

    // ========================================
    // PHASE 1: Start workflow, wait for step 3 to complete, then KILL the coordinator thread
    // ========================================
    logger.info("=== PHASE 1: Starting workflow  ===");

    shouldBlockBeforeStep4 = true;  // Enable blocking before step 4

    coordinator1 = new Coordinator(stateManager);
    coordinator1.register(SequentialOrderWorkflow.class, OrderReceivedEvent.class);
    coordinator1.start();

    // Publish trigger event
    stateManager.append(createTriggerEvent(orderId));

    // Wait for chargePayment to complete (step 3 done, event persisted)
    logger.info("Waiting for chargePayment to complete...");
    boolean stepCompleted = stepCompletedLatch.await(10, TimeUnit.SECONDS);
    assertThat(stepCompleted).as("chargePayment should complete before kill").isTrue();

    // Give a tiny moment to ensure event is fully persisted
    Thread.sleep(50);

    // AGGRESSIVELY KILL the workflow thread
    logger.info("=== KILLING workflow thread: {} ===", workflowThread.getName());
    workflowThread.interrupt();

    // Also stop the coordinator
    coordinator1.stop();

    // Wait for the thread to die
    Thread.sleep(200);

    // Verify steps 1-3 executed exactly once, steps 4-5 did not execute
    logger.info("=== Verifying step execution counts after KILL ===");
    assertThat(stepExecutionCounts.get("checkInventory").get())
      .as("checkInventory should have executed once")
      .isEqualTo(1);
    assertThat(stepExecutionCounts.get("reserveStock").get())
      .as("reserveStock should have executed once")
      .isEqualTo(1);
    assertThat(stepExecutionCounts.get("chargePayment").get())
      .as("chargePayment should have executed once")
      .isEqualTo(1);

    // Steps 4-5 should NOT have executed due to kill
    assertThat(stepExecutionCounts.get("createShipment"))
      .as("createShipment should not have executed before kill")
      .isNull();
    assertThat(stepExecutionCounts.get("sendConfirmation"))
      .as("sendConfirmation should not have executed before kill")
      .isNull();

    // ========================================
    // PHASE 2: Recovery - new Coordinator with same StateManager
    // ========================================
    logger.info("=== PHASE 2: Starting recovery with new coordinator ===");

    shouldBlockBeforeStep4 = false;  // Don't block during recovery

    coordinator2 = new Coordinator(stateManager);  // SAME stateManager!
    coordinator2.register(SequentialOrderWorkflow.class, OrderReceivedEvent.class);
    coordinator2.start();

    // Re-publish same trigger event (same orderId = same workflowId)
    // The new coordinator's consumedMessages is empty, so it will process this event
    stateManager.append(createTriggerEvent(orderId));

    // Wait for workflow to complete successfully
    await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
      assertThat(coordinator2.getHistory()).hasSize(1);
      assertThat(coordinator2.getRunning()).isEmpty();
    });

    // ========================================
    // PHASE 3: Verification
    // ========================================
    logger.info("=== PHASE 3: Verifying recovery worked correctly ===");

    WorkflowContext completedContext = coordinator2.getHistory().iterator().next();

    // Verify all steps are in the history
    assertThat(completedContext.getStepHistory())
      .as("All 5 steps should be in the completed workflow")
      .contains("checkInventory", "reserveStock", "chargePayment", "createShipment", "sendConfirmation");

    // CRITICAL: Steps 1-3 should still have count=1 (NOT re-executed during recovery)
    assertThat(stepExecutionCounts.get("checkInventory").get())
      .as("checkInventory should NOT have been re-executed")
      .isEqualTo(1);
    assertThat(stepExecutionCounts.get("reserveStock").get())
      .as("reserveStock should NOT have been re-executed")
      .isEqualTo(1);
    assertThat(stepExecutionCounts.get("chargePayment").get())
      .as("chargePayment should NOT have been re-executed")
      .isEqualTo(1);

    // Steps 4-5 should now have count=1 (executed during recovery)
    assertThat(stepExecutionCounts.get("createShipment").get())
      .as("createShipment should have executed once during recovery")
      .isEqualTo(1);
    assertThat(stepExecutionCounts.get("sendConfirmation").get())
      .as("sendConfirmation should have executed once during recovery")
      .isEqualTo(1);

    // Log final step history
    logger.info("=== Final Step History ===");
    int i = 1;
    for (String step : completedContext.getStepHistory()) {
      logger.info("  {}. {}", i++, step);
    }
    logger.info("===========================");

    // Log execution counts
    logger.info("=== Step Execution Counts ===");
    stepExecutionCounts.forEach((step, count) ->
      logger.info("  {} executed {} time(s)", step, count.get()));
    logger.info("=============================");
  }

  // ============== HELPER ==============

  private EventMessage createTriggerEvent(String orderId) {
    OrderReceivedEvent event = new OrderReceivedEvent(
      orderId,
      "CUST-123",
      "LAPTOP-PRO-15",
      2,
      BigDecimal.valueOf(2499.98)
    );
    return new GenericEventMessage(
      MessageType.fromString(event.getClass().getTypeName() + "#0.1"),
      event
    );
  }
}
