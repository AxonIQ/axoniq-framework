package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple.SimpleContext;
import io.axoniq.workflow.dsl.simple.SimpleDefinition;
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer;
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;
import io.axoniq.workflow.runtime.engine.Coordinator;
import io.axoniq.workflow.runtime.engine.StateManager;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static io.axoniq.workflow.dsl.simple.Payload.payload;
import static io.axoniq.workflow.runtime.DelayedPublisher.Schedule.ofMillis;
import static io.axoniq.workflow.runtime.context.DefaultEventNameCustomizer.Builder.eventName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests timeout handling and compensation in a Loan Application workflow.
 */
class LoanApplicationTimeoutTest {

  private static final Logger logger = LoggerFactory.getLogger(LoanApplicationTimeoutTest.class);

  private StateManager stateManager;
  private Coordinator coordinator;
  private DelayedPublisher delayedPublisher;

  // ============== MOCK SERVICES ==============

  static class CreditBureauService {
    /**
     * Simulates external credit bureau API that may be slow/unresponsive.
     */
    static CreditScore checkCredit(String applicantId, long delayMs) {
      logger.info("[CreditBureau] Checking credit for applicant: {}", applicantId);
      sleep(delayMs); // Simulate slow external API
      logger.info("[CreditBureau] Credit check complete for: {}", applicantId);
      return new CreditScore(applicantId, 720, "GOOD");
    }
  }

  static class ApplicationService {
    static void createPendingApplication(String applicationId, String applicantId, BigDecimal amount) {
      logger.info("[ApplicationService] Creating pending application {} for {} - ${}",
        applicationId, applicantId, amount);
      sleep(100);
      logger.info("[ApplicationService] Application {} created", applicationId);
    }

    static void cancelApplication(String applicationId, String reason) {
      logger.info("[ApplicationService] Canceling application {}: {}", applicationId, reason);
      sleep(100);
      logger.info("[ApplicationService] Application {} canceled", applicationId);
    }

    static void approveApplication(String applicationId) {
      logger.info("[ApplicationService] Approving application {}", applicationId);
      sleep(100);
      logger.info("[ApplicationService] Application {} approved", applicationId);
    }
  }

  static class NotificationService {
    static void notifyApplicant(String applicantId, String message) {
      logger.info("[NotificationService] Notifying {}: {}", applicantId, message);
      sleep(50);
    }
  }

  // ============== DOMAIN RECORDS ==============

  record LoanApplicationEvent(
    String applicationId,
    String applicantId,
    BigDecimal requestedAmount,
    long creditCheckDelayMs // for testing: how long credit bureau takes
  ) {
  }

  record CreditScore(String applicantId, int score, String rating) {
  }

  // ============== WORKFLOW DEFINITION ==============

  static class LoanApplicationWorkflow implements SimpleDefinition {

    private static final Duration CREDIT_CHECK_TIMEOUT = Duration.ofMillis(500);

    @Override
    public String association(@Nonnull Map<String, Object> trigger) {
      return "loan-" + trigger.getOrDefault("applicationId", UUID.randomUUID().toString());
    }

    @Override
    public void execute(SimpleContext context) {
      Map<String, Object> trigger = context.getPayload();
      String applicationId = (String) trigger.get("applicationId");
      String applicantId = (String) trigger.get("applicantId");
      BigDecimal amount = (BigDecimal) trigger.get("requestedAmount");
      long creditCheckDelay = ((Number) trigger.get("creditCheckDelayMs")).longValue();

      logger.info("========== Starting Loan Application: {} ==========", applicationId);

      // Step 1: Create pending application
      context.execute("createApplication",
        Map.of("applicationId", applicationId, "applicantId", applicantId, "amount", amount),
        Void.class,
        p -> {
          ApplicationService.createPendingApplication(
            (String) p.get("applicationId"),
            (String) p.get("applicantId"),
            (BigDecimal) p.get("amount")
          );
          return null;
        });

      // Step 2: Call external credit bureau with timeout
      CreditScore creditScore;
      try {
        creditScore = callCreditBureauWithTimeout(context, applicantId, creditCheckDelay);
      } catch (CompletionException e) {
        if (e.getCause() instanceof TimeoutException) {
          logger.warn("--- Credit check timed out! Starting compensation ---");

          // COMPENSATION: Cancel application due to timeout
          context.execute("cancelApplication",
            Map.of("applicationId", applicationId),
            Void.class,
            p -> {
              ApplicationService.cancelApplication(
                (String) p.get("applicationId"),
                "Credit bureau timeout"
              );
              return null;
            });

          // COMPENSATION: Notify applicant
          context.execute("notifyTimeout",
            Map.of("applicantId", applicantId, "applicationId", applicationId),
            Void.class,
            p -> {
              NotificationService.notifyApplicant(
                (String) p.get("applicantId"),
                "Your loan application " + p.get("applicationId") + " could not be processed. " +
                  "Please try again later."
              );
              return null;
            });

          logger.info("--- Compensation complete ---");
          return;
        }
        throw e;
      }

      // Step 3: Process based on credit score
      if (creditScore.score() >= 650) {
        context.execute("approveApplication",
          Map.of("applicationId", applicationId, "applicantId", applicantId),
          Void.class,
          p -> {
            ApplicationService.approveApplication((String) p.get("applicationId"));
            return null;
          });

        context.execute("notifyApproval",
          Map.of("applicantId", applicantId, "applicationId", applicationId),
          Void.class,
          p -> {
            NotificationService.notifyApplicant(
              (String) p.get("applicantId"),
              "Congratulations! Your loan application " + p.get("applicationId") + " has been approved."
            );
            return null;
          });
      } else {
        context.execute("rejectApplication",
          Map.of("applicationId", applicationId, "creditScore", creditScore.score()),
          Void.class,
          p -> {
            ApplicationService.cancelApplication(
              (String) p.get("applicationId"),
              "Credit score too low: " + p.get("creditScore")
            );
            return null;
          });

        context.execute("notifyRejection",
          Map.of("applicantId", applicantId, "applicationId", applicationId),
          Void.class,
          p -> {
            NotificationService.notifyApplicant(
              (String) p.get("applicantId"),
              "Your loan application " + p.get("applicationId") + " was not approved due to credit requirements."
            );
            return null;
          });
      }

      logger.info("========== Loan Application Complete: {} ==========", applicationId);
    }

    private CreditScore callCreditBureauWithTimeout(SimpleContext context, String applicantId, long delayMs) {
      String stepSpecificKey = "__creditScore";

      var result = context.execute(
        "checkCredit",
        payload()
          .with("applicantId", applicantId)
          .with("delayMs", delayMs)
          .getValues(),
        p -> {
          var payload = payload(p);
          CreditScore score = CreditBureauService.checkCredit(
            payload.get("applicantId"),
            payload.get("delayMs")
          );
          return payload(stepSpecificKey, score).getValues();
        },
        PayloadReducer.local(),
        PayloadReducer.all(),
        CREDIT_CHECK_TIMEOUT,
        eventName()
      ).join();

      return (CreditScore) result.get(stepSpecificKey);
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
  }

  @AfterEach
  void tearDown() {
    stateManager.printPayloads();
    coordinator.stop();
  }

  @Test
  void shouldCompensateWhenCreditCheckTimesOut() {
    // Given - loan application where credit check will timeout
    coordinator.declarative().register(new QualifiedName(LoanApplicationEvent.class), new LoanApplicationWorkflow());

    delayedPublisher.addSchedules(List.of(
      ofMillis(100, new LoanApplicationEvent(
        "LOAN-001",
        "APPLICANT-123",
        BigDecimal.valueOf(25000),
        2000  // Credit check takes 2000ms, but timeout is 500ms
      ))
    ));

    // When
    coordinator.start();
    delayedPublisher.start();

    // Then - workflow should complete with compensation
    await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
      assertThat(coordinator.getHistory()).hasSize(1);
      assertThat(coordinator.getRunning()).isEmpty();
    });

    WorkflowContext context = coordinator.getHistory().iterator().next();

    // Verify application was created
    assertThat(context.getStepHistory()).contains("createApplication");

    // Verify credit check was attempted (and timed out)
    assertThat(context.getStepHistory()).contains("checkCredit");

    // Verify compensation ran
    assertThat(context.getStepHistory())
      .contains("cancelApplication", "notifyTimeout");

    // Verify approval steps did NOT run
    assertThat(context.getStepHistory())
      .doesNotContain("approveApplication", "notifyApproval");

    printStepHistory(context);
  }

  @Test
  void shouldCompleteSuccessfullyWhenCreditCheckRespondsInTime() {
    // Given - loan application where credit check responds quickly
    coordinator.declarative().register(new QualifiedName(LoanApplicationEvent.class), new LoanApplicationWorkflow());

    delayedPublisher.addSchedules(List.of(
      ofMillis(100, new LoanApplicationEvent(
        "LOAN-002",
        "APPLICANT-456",
        BigDecimal.valueOf(15000),
        100  // Credit check takes 100ms, well within 500ms timeout
      ))
    ));

    // When
    coordinator.start();
    delayedPublisher.start();

    // Then - workflow should complete successfully
    await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
      assertThat(coordinator.getHistory()).hasSize(1);
      assertThat(coordinator.getRunning()).isEmpty();
    });

    WorkflowContext context = coordinator.getHistory().iterator().next();

    // Verify happy path executed
    assertThat(context.getStepHistory())
      .contains("createApplication", "checkCredit", "approveApplication", "notifyApproval");

    // Verify compensation did NOT run
    assertThat(context.getStepHistory())
      .doesNotContain("notifyTimeout");

    printStepHistory(context);
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
