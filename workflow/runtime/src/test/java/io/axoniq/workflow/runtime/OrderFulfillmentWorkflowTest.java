package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple.SimpleContext;
import io.axoniq.workflow.dsl.simple.SimpleDefinition;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.Coordinator;
import io.axoniq.workflow.runtime.engine.StateManager;
import jakarta.annotation.Nonnull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.axoniq.workflow.dsl.simple.Payload.payload;
import static io.axoniq.workflow.runtime.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class OrderFulfillmentWorkflowTest {

  private static final Logger logger = LoggerFactory.getLogger(OrderFulfillmentWorkflowTest.class);

  private StateManager stateManager;
  private Coordinator coordinator;
  private DelayedPublisher delayedPublisher;

  // ============== MOCK SERVICES ==============

  static class InventoryService {
    static InventoryResult checkInventory(String productId, int quantity) {
      logger.info("[InventoryService] Checking inventory for {} x{}", productId, quantity);
      randomDelay(1500, 2500);
      int available = 100; // mock: always have 100 in stock
      boolean inStock = available >= quantity;
      logger.info("[InventoryService] Product {} - Available: {}, Requested: {}, InStock: {}",
        productId, available, quantity, inStock);
      return new InventoryResult(productId, available, inStock);
    }

    static void reserveStock(String productId, int quantity) {
      logger.info("[InventoryService] Reserving {} units of {}", quantity, productId);
      randomDelay(500, 1000);
      logger.info("[InventoryService] Stock reserved successfully");
    }

    static void releaseStock(String productId, int quantity) {
      logger.info("[InventoryService] Releasing {} units of {}", quantity, productId);
      randomDelay(300, 600);
      logger.info("[InventoryService] Stock released");
    }
  }

  static class PaymentService {
    static PaymentValidation validatePayment(String customerId, BigDecimal amount) {
      logger.info("[PaymentService] Validating payment for customer {} amount ${}", customerId, amount);
      randomDelay(2000, 3000);
      boolean valid = amount.compareTo(BigDecimal.valueOf(10000)) < 0; // reject orders > $10k
      logger.info("[PaymentService] Payment validation: {}", valid ? "APPROVED" : "REJECTED");
      return new PaymentValidation(customerId, valid, valid ? "APPROVED" : "LIMIT_EXCEEDED");
    }

    static PaymentResult chargePayment(String customerId, BigDecimal amount, boolean shouldFail) {
      logger.info("[PaymentService] Charging ${} to customer {}", amount, customerId);
      randomDelay(1000, 2000);
      if (shouldFail) {
        throw new RuntimeException("Payment gateway timeout - card declined");
      }
      String transactionId = "TXN-" + UUID.randomUUID().toString().substring(0, 8);
      logger.info("[PaymentService] Payment successful - Transaction: {}", transactionId);
      return new PaymentResult(transactionId, amount, "COMPLETED");
    }

    static void refundPayment(String transactionId, BigDecimal amount) {
      logger.info("[PaymentService] Refunding ${} for transaction {}", amount, transactionId);
      randomDelay(500, 1000);
      logger.info("[PaymentService] Refund processed");
    }
  }

  static class FraudService {
    static FraudCheckResult checkFraud(String customerId, String productId, BigDecimal amount) {
      logger.info("[FraudService] Running fraud check for customer {} - ${}", customerId, amount);
      randomDelay(1800, 2800);
      int riskScore = (int) (Math.random() * 30); // low risk score 0-30
      boolean passed = riskScore < 70;
      logger.info("[FraudService] Fraud check complete - Risk score: {}, Passed: {}", riskScore, passed);
      return new FraudCheckResult(riskScore, passed);
    }
  }

  static class ShippingService {
    static ShipmentResult createShipment(String orderId, String productId, int quantity) {
      logger.info("[ShippingService] Creating shipment for order {}", orderId);
      randomDelay(800, 1500);
      String trackingNumber = "TRACK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
      logger.info("[ShippingService] Shipment created - Tracking: {}", trackingNumber);
      return new ShipmentResult(trackingNumber, "LABEL_CREATED");
    }
  }

  static class NotificationService {
    static void notifyCustomer(String customerId, String message) {
      logger.info("[NotificationService] Sending notification to customer {}: {}", customerId, message);
      randomDelay(200, 400);
      logger.info("[NotificationService] Notification sent");
    }
  }

  // ============== DOMAIN RECORDS ==============

  record Order(String orderId, String customerId, String productId, int quantity, BigDecimal totalAmount) {
  }

  record InventoryResult(String productId, int available, boolean inStock) {
  }

  record PaymentValidation(String customerId, boolean valid, String status) {
  }

  record PaymentResult(String transactionId, BigDecimal amount, String status) {
  }

  record FraudCheckResult(int riskScore, boolean passed) {
  }

  record ShipmentResult(String trackingNumber, String status) {
  }

  // Trigger events
  record OrderReceivedEvent(String orderId, String customerId, String productId, int quantity, BigDecimal totalAmount,
                            boolean simulatePaymentFailure) {
  }

  record ManagerApprovalEvent(String orderId, String managerId, boolean approved, String comment) {
  }

  // ============== WORKFLOW DEFINITION ==============

  static class OrderFulfillmentWorkflow extends SimpleDefinition.Type {

    @Override
    public String workflowId(Map<String, Object> trigger) {
      return "order-" + trigger.getOrDefault("orderId", UUID.randomUUID().toString());
    }

    @Override
    public void execute(@Nonnull SimpleContext context) {
      // Extract order data from trigger event payload
      var triggerData = payload(context);
      Order order = new Order(
        triggerData.get("orderId"),
        triggerData.get("customerId"),
        triggerData.get("productId"),
        triggerData.get("quantity"),
        triggerData.get("totalAmount")
      );
      boolean simulatePaymentFailure = Boolean.TRUE.equals(triggerData.get("simulatePaymentFailure"));

      logger.info("========== Starting Order Fulfillment: {} ==========", order.orderId());

      // ==========================================
      // PHASE 1: PARALLEL VALIDATION
      // Run inventory check, payment validation, and fraud check in parallel
      // ==========================================
      logger.info("--- Phase 1: Parallel Validation ---");

      CompletableFuture<InventoryResult> inventoryFuture = CompletableFuture.supplyAsync(() ->
        context.execute("checkInventory",
          payload()
            .with("productId", order.productId())
            .with("quantity", order.quantity()),
          InventoryResult.class,
          payload -> InventoryService.checkInventory(
            payload.get("productId"),
            payload.get("quantity")
          ))
      );

      CompletableFuture<PaymentValidation> paymentValidationFuture = CompletableFuture.supplyAsync(() ->
        context.execute("validatePayment",
          payload()
            .with("customerId", order.customerId())
            .with("amount", order.totalAmount()),
          PaymentValidation.class,
          payload -> PaymentService.validatePayment(
            payload.get("customerId"),
            payload.get("amount")
          ))
      );

      CompletableFuture<FraudCheckResult> fraudFuture = CompletableFuture.supplyAsync(() ->
        context.execute("checkFraud",
          payload(context),                   // <-- Just pass the entire order instead of selecting the fields
          FraudCheckResult.class,
          payload -> FraudService.checkFraud(
            payload.get("customerId"),
            payload.get("productId"),
            payload.get("amount")
          ))
      );

      // Wait for all validations to complete
      InventoryResult inventory = inventoryFuture.join();
      PaymentValidation paymentValidation = paymentValidationFuture.join();
      FraudCheckResult fraudCheck = fraudFuture.join();

      logger.info("--- Phase 1 Complete: Inventory={}, Payment={}, Fraud={} ---",
        inventory.inStock(), paymentValidation.valid(), fraudCheck.passed());

      // Check if all validations passed
      if (!inventory.inStock()) {
        context.execute("notifyOutOfStock",
          payload()
            .with("customerId", order.customerId())
            .with("productId", order.productId()),
          payload -> {
            NotificationService.notifyCustomer(
              payload.get("customerId"),
              "Sorry, product " + payload.get("productId") + " is out of stock"
            );
          });
        return;
      }

      if (!paymentValidation.valid() || !fraudCheck.passed()) {
        context.execute("notifyOrderRejected",
          payload()
            .with("customerId", order.customerId())
            .with("reason", !paymentValidation.valid() ? "Payment validation failed" : "Fraud check failed"),
          payload -> {
            NotificationService.notifyCustomer(
              payload.get("customerId"),
              "Order rejected: " + payload.get("reason")
            );
          });
        return;
      }

      // ==========================================
      // PHASE 2: SEQUENTIAL PROCESSING
      // Reserve stock, charge payment, create shipment
      // ==========================================
      logger.info("--- Phase 2: Sequential Processing ---");

      // Step 1: Reserve Stock
      context.execute("reserveStock",
        payload()
          .with("productId", order.productId())
          .with("quantity", order.quantity()),
        payload -> {
          InventoryService.reserveStock(
            payload.get("productId"),
            payload.get("quantity")
          );
        });

      // Step 2: Charge Payment (with error handling)
      PaymentResult paymentResult;
      try {
        paymentResult = context.execute("chargePayment",
          payload()
            .with("customerId", order.customerId())
            .with("amount", order.totalAmount())
            .with("simulateFailure", simulatePaymentFailure),
          PaymentResult.class,
          payload -> PaymentService.chargePayment(
            payload.get("customerId"),
            payload.get("amount"),
            payload.get("simulateFailure")
          ));
      } catch (CompletionException e) {
        // ==========================================
        // COMPENSATION: Payment failed, rollback
        // ==========================================
        logger.error("--- Payment Failed! Starting Compensation ---");

        context.execute("releaseStock",
          payload()
            .with("productId", order.productId())
            .with("quantity", order.quantity()),
          payload -> {
            InventoryService.releaseStock(
              payload.get("productId"),
              payload.get("quantity")
            );
          });

        context.execute("notifyPaymentFailed",
          payload()
            .with("customerId", order.customerId())
            .with("orderId", order.orderId()),
          payload -> {
            NotificationService.notifyCustomer(
              payload.get("customerId"),
              "Payment failed for order " + payload.get("orderId") + ". Please try again."
            );
          });

        logger.info("--- Compensation Complete ---");
        return;
      }

      // Step 3: Create Shipment
      ShipmentResult shipment = context.execute("createShipment",
        payload()
          .with("orderId", order.orderId())
          .with("productId", order.productId())
          .with("quantity", order.quantity()),
        ShipmentResult.class,
        payload -> ShippingService.createShipment(
          payload.get("orderId"),
          payload.get("productId"),
          payload.get("quantity")
        ));

      // Step 4: Send Confirmation
      context.execute("sendConfirmation",
        payload()
          .with("customerId", order.customerId())
          .with("orderId", order.orderId())
          .with("trackingNumber", shipment.trackingNumber())
          .with("transactionId", paymentResult.transactionId()),
        payload -> {
          NotificationService.notifyCustomer(
            payload.get("customerId"),
            String.format("Order %s confirmed! Transaction: %s, Tracking: %s",
              payload.get("orderId"), payload.get("transactionId"), payload.get("trackingNumber")
            )
          );
        });

      logger.info("========== Order Fulfillment Complete: {} ==========", order.orderId());
    }
  }

  // ============== WORKFLOW WITH HUMAN APPROVAL ==============

  static class OrderWithApprovalWorkflow extends SimpleDefinition.Type {

    private static final BigDecimal APPROVAL_THRESHOLD = BigDecimal.valueOf(1000);

    @Override
    public String workflowId(Map<String, Object> trigger) {
      return "order-approval-" + trigger.getOrDefault("orderId", UUID.randomUUID().toString());
    }

    @Override
    public void execute(@Nonnull SimpleContext context) {
      // Extract order details from the trigger event payload
      var triggerData = payload(context);
      Order order = new Order(
        triggerData.get("orderId"),
        triggerData.get("customerId"),
        triggerData.get("productId"),
        triggerData.get("quantity"),
        triggerData.get("totalAmount")
      );
      logger.info("========== Starting Order With Approval: {} ==========", order.orderId());

      // ==========================================
      // PHASE 1: PARALLEL VALIDATION (same as before)
      // ==========================================
      logger.info("--- Phase 1: Parallel Validation ---");

      CompletableFuture<InventoryResult> inventoryFuture = CompletableFuture.supplyAsync(() ->
        context.execute("checkInventory",
          payload()
            .with("productId", order.productId())
            .with("quantity", order.quantity()),
          InventoryResult.class,
          payload -> InventoryService.checkInventory(
            payload.get("productId"),
            payload.get("quantity")
          ))
      );

      CompletableFuture<FraudCheckResult> fraudFuture = CompletableFuture.supplyAsync(() ->
        context.execute("checkFraud",
          payload()
            .with("customerId", order.customerId())
            .with("productId", order.productId())
            .with("amount", order.totalAmount()),
          FraudCheckResult.class,
          payload -> FraudService.checkFraud(
            payload.get("customerId"),
            payload.get("productId"),
            payload.get("amount")
          ))
      );

      InventoryResult inventory = inventoryFuture.join();
      FraudCheckResult fraudCheck = fraudFuture.join();

      if (!inventory.inStock() || !fraudCheck.passed()) {
        logger.warn("Validation failed - InStock: {}, FraudCheck: {}", inventory.inStock(), fraudCheck.passed());
        return;
      }

      // ==========================================
      // PHASE 2: HUMAN APPROVAL (for high-value orders)
      // ==========================================
      if (order.totalAmount().compareTo(APPROVAL_THRESHOLD) >= 0) {
        logger.info("--- Phase 2: Waiting for Manager Approval (order > ${}) ---", APPROVAL_THRESHOLD);

        // Notify manager that approval is needed
        context.execute("requestManagerApproval",
          payload()
            .with("orderId", order.orderId())
            .with("amount", order.totalAmount())
            .with("customerId", order.customerId()),
          payload -> {
            logger.info("[NotificationService] Sending approval request to manager for order {} (${}) ",
              payload.get("orderId"), payload.get("amount"));
            randomDelay(200, 400);
          });

        // *** WAIT FOR HUMAN INPUT ***
        // This blocks until ManagerApprovalEvent arrives (with matching orderId)
        logger.info(">>> Waiting for manager approval event... (timeout: 30s)");
        ManagerApprovalEvent approval = context.waitForEvent(
          "awaitManagerApproval",
          ManagerApprovalEvent.class,
          event -> event.orderId().equals(order.orderId()),  // predicate: match order ID
          Duration.ofSeconds(30)
        );

        logger.info(">>> Received manager approval: approved={}, comment={}", approval.approved(), approval.comment());

        if (!approval.approved()) {
          context.execute("notifyApprovalRejected",
            payload()
              .with("customerId", order.customerId())
              .with("orderId", order.orderId())
              .with("comment", approval.comment()),
            payload -> {
              NotificationService.notifyCustomer(
                payload.get("customerId"),
                "Order " + payload.get("orderId") + " rejected by manager: " + payload.get("comment")
              );
            });
          return;
        }
      }

      // ==========================================
      // PHASE 3: SEQUENTIAL PROCESSING
      // ==========================================
      logger.info("--- Phase 3: Processing Order ---");

      context.execute("reserveStock",
        payload("productId", order.productId(), "quantity", order.quantity()),
        payload -> {
          InventoryService.reserveStock(
            payload.get("productId"),
            payload.get("quantity"));
        });

      PaymentResult payment = context.execute("chargePayment",
        payload()
          .with("customerId", order.customerId())
          .with("amount", order.totalAmount())
          .with("simulateFailure", false),
        PaymentResult.class,
        payload -> PaymentService.chargePayment(
          payload.get("customerId"),
          payload.get("amount"),
          payload.get("simulateFailure")
        ));

      ShipmentResult shipment = context.execute("createShipment",
        payload().with("orderId", order.orderId()).with("productId", order.productId()).with("quantity", order.quantity()),
        ShipmentResult.class,
        payload -> ShippingService.createShipment(
          payload.get("orderId"),
          payload.get("productId"),
          payload.get("quantity")
        ));

      context.execute("sendConfirmation",
        payload()
          .with("customerId", order.customerId())
          .with("trackingNumber", shipment.trackingNumber())
          .with("transactionId", payment.transactionId()),
        payload -> {
          NotificationService.notifyCustomer(
            payload.get("customerId"),
            "Order confirmed! Tracking: " + payload.get("trackingNumber")
          );
        });

      logger.info("========== Order With Approval Complete: {} ==========", order.orderId());
    }
  }

  // ============== HELPER ==============

  private static void randomDelay(int minMs, int maxMs) {
    try {
      Thread.sleep(minMs + (int) (Math.random() * (maxMs - minMs)));
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
  }

  @AfterEach
  void printEvents() {
    stateManager.printPayloads();
  }

  @Test
  void shouldCompleteOrderSuccessfully() {
    // Given - register workflow to start on OrderReceivedEvent
    coordinator.register(OrderFulfillmentWorkflow.class, OrderReceivedEvent.class);

    // Schedule the trigger event
    delayedPublisher.addSchedules(List.of(
      ofMillis(100, new OrderReceivedEvent(
        "ORD-001",
        "CUST-123",
        "LAPTOP-PRO-15",
        2,
        BigDecimal.valueOf(2499.98),
        false  // don't simulate payment failure
      ))
    ));

    // When - start coordinator and publish events
    coordinator.start();
    delayedPublisher.start();

    // Then - wait for workflow to complete
    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
      assertThat(coordinator.getHistory()).hasSize(1);
      assertThat(coordinator.getRunning()).isEmpty();
    });

    WorkflowContext context = coordinator.getHistory().iterator().next();

    // Verify all steps executed in correct order
    assertThat(context.getStepHistory())
      .contains("checkInventory", "validatePayment", "checkFraud")  // parallel validation
      .contains("reserveStock", "chargePayment", "createShipment", "sendConfirmation");  // sequential

    // Verify compensation steps were NOT taken
    assertThat(context.getStepHistory())
      .doesNotContain("releaseStock", "notifyPaymentFailed", "notifyOutOfStock", "notifyOrderRejected");

    printStepHistory(context);
    coordinator.stop();
  }

  @Test
  void shouldCompensateWhenPaymentFails() {
    // Given - register workflow to start on OrderReceivedEvent
    coordinator.register(OrderFulfillmentWorkflow.class, OrderReceivedEvent.class);

    // Schedule the trigger event with payment failure simulation
    delayedPublisher.addSchedules(List.of(
      ofMillis(100, new OrderReceivedEvent(
        "ORD-002",
        "CUST-456",
        "PHONE-ULTRA",
        1,
        BigDecimal.valueOf(1299.99),
        true  // simulate payment failure
      ))
    ));

    // When - start coordinator and publish events
    coordinator.start();
    delayedPublisher.start();

    // Then - wait for workflow to complete
    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
      assertThat(coordinator.getHistory()).hasSize(1);
      assertThat(coordinator.getRunning()).isEmpty();
    });

    WorkflowContext context = coordinator.getHistory().iterator().next();

    // Verify parallel validation ran
    assertThat(context.getStepHistory())
      .contains("checkInventory", "validatePayment", "checkFraud");

    // Verify stock was reserved then released (compensation)
    assertThat(context.getStepHistory())
      .contains("reserveStock", "chargePayment", "releaseStock", "notifyPaymentFailed");

    // Verify shipment was NOT created (stopped at payment)
    assertThat(context.getStepHistory())
      .doesNotContain("createShipment", "sendConfirmation");

    printStepHistory(context);
    coordinator.stop();
  }

  @Test
  void shouldWaitForManagerApprovalAndComplete() {
    // Given - high value order requiring manager approval
    Order order = new Order(
      "ORD-003",
      "CUST-789",
      "SERVER-RACK-42U",
      1,
      BigDecimal.valueOf(4999.99)  // Over $1000 threshold - requires approval
    );

    // Register workflow to start on OrderReceivedEvent
    coordinator.register(OrderWithApprovalWorkflow.class, OrderReceivedEvent.class);

    // Schedule events:
    // 1. Order submitted immediately
    // 2. Manager approval arrives after 5 seconds (simulating human input)
    delayedPublisher.addSchedules(List.of(
      ofMillis(100, new OrderReceivedEvent(
        order.orderId(), order.customerId(), order.productId(), order.quantity(), order.totalAmount(), false
      )),
      ofMillis(5000, new ManagerApprovalEvent(
        order.orderId(),
        "MGR-001",
        true,  // approved
        "Approved - verified customer is enterprise client"
      ))
    ));

    // When - start coordinator and publish events
    coordinator.start();
    delayedPublisher.start();

    // Then - wait for workflow to complete (with approval)
    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
      assertThat(coordinator.getHistory()).hasSize(1);
      assertThat(coordinator.getRunning()).isEmpty();
    });

    // Verify workflow steps
    WorkflowContext completedContext = coordinator.getHistory().iterator().next();

    // Verify validation ran in parallel
    assertThat(completedContext.getStepHistory())
      .contains("checkInventory", "checkFraud");

    // Verify approval flow
    assertThat(completedContext.getStepHistory())
      .contains("requestManagerApproval", "awaitManagerApproval");

    // Verify order was processed after approval
    assertThat(completedContext.getStepHistory())
      .contains("reserveStock", "chargePayment", "createShipment", "sendConfirmation");

    logger.info("=== Workflow with Human Approval - Step History ===");
    int i = 1;
    for (String step : completedContext.getStepHistory()) {
      logger.info("  {}. {}", i++, step);
    }
    logger.info("==================================================");

    coordinator.stop();
  }

  @Test
  void shouldRejectOrderWhenManagerDeniesApproval() {
    // Given - high value order that will be rejected
    Order order = new Order(
      "ORD-004",
      "CUST-SUSPECT",
      "GOLD-BARS-10KG",
      5,
      BigDecimal.valueOf(8500.00)  // Suspicious order
    );

    // Register workflow to start on OrderReceivedEvent
    coordinator.register(OrderWithApprovalWorkflow.class, OrderReceivedEvent.class);

    delayedPublisher.addSchedules(List.of(
      ofMillis(100, new OrderReceivedEvent(
        order.orderId(), order.customerId(), order.productId(), order.quantity(), order.totalAmount(), false
      )),
      ofMillis(5000, new ManagerApprovalEvent(
        order.orderId(),
        "MGR-002",
        false,  // REJECTED
        "Suspicious order - customer flagged for review"
      ))
    ));

    // When
    coordinator.start();
    delayedPublisher.start();

    // Then - wait for workflow to complete
    await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
      assertThat(coordinator.getHistory()).hasSize(1);
    });

    WorkflowContext completedContext = coordinator.getHistory().iterator().next();

    // Verify approval was requested and awaited
    assertThat(completedContext.getStepHistory())
      .contains("requestManagerApproval", "awaitManagerApproval");

    // Verify rejection notification was sent
    assertThat(completedContext.getStepHistory())
      .contains("notifyApprovalRejected");

    // Verify order was NOT processed
    assertThat(completedContext.getStepHistory())
      .doesNotContain("reserveStock", "chargePayment", "createShipment", "sendConfirmation");

    logger.info("=== Rejected Order - Step History ===");
    int i = 1;
    for (String step : completedContext.getStepHistory()) {
      logger.info("  {}. {}", i++, step);
    }
    logger.info("=====================================");

    coordinator.stop();
  }

  private void printStepHistory(WorkflowContext context) {
    logger.info("=== Step History ===");
    int i = 1;
    for (String step : context.getStepHistory()) {
      logger.info("  {}. {}", i++, step);
    }
    logger.info("====================");
  }
}
