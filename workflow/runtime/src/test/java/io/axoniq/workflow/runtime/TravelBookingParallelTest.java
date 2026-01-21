package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple.SimpleContext;
import io.axoniq.workflow.dsl.simple.SimpleDefinition;
import io.axoniq.workflow.runtime.engine.StateManager;
import io.axoniq.workflow.runtime.engine.StepFailedException;
import io.axoniq.workflow.runtime.engine.WorkflowEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;

class TravelBookingParallelTest {

  private static final Logger logger = LoggerFactory.getLogger(TravelBookingParallelTest.class);

  private StateManager stateManager;
  private WorkflowEngine engine;

  @BeforeEach
  void setUp() {
    stateManager = new StateManager();
    engine = new WorkflowEngine(stateManager);
  }

  @AfterEach
  void printEvents() {
    stateManager.printPayloads();
  }

  static class TravelBookingWorkflow extends SimpleDefinition.Type {

    private static void randomDelay(int minMs, int maxMs) {
      try {
        Thread.sleep(minMs + (int) (Math.random() * (maxMs - minMs)));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public String workflowId(Map<String, Object> trigger) {
      return "travel-booking-" + trigger.getOrDefault("bookingId", "default");
    }

    @Override
    public void execute(SimpleContext context) {
      // 1. PARALLEL: Start both reservations
      CompletableFuture<String> flightFuture = CompletableFuture.supplyAsync(() ->
        context.execute("reserveFlight", String.class, () -> {
          logger.info("Reserving flight...");
          randomDelay(2000, 3000);
          return "FL-123";
        })
      );
      CompletableFuture<String> hotelFuture = CompletableFuture.supplyAsync(() ->
        context.execute("reserveHotel", String.class, () -> {
          logger.info("Reserving hotel...");
          randomDelay(2000, 3000);
          return "HT-456";
        })
      );

      // 2. WAIT & PRINT: Join both results
      String flightCode = flightFuture.join();
      String hotelCode = hotelFuture.join();
      logger.info("Reservations complete - Flight: {}, Hotel: {}", flightCode, hotelCode);

      // 3. FAILING STEP with try-catch
      try {
        context.execute("processPayment", () -> {
          throw new RuntimeException("Payment gateway unavailable");
        });
        // Success path (won't execute)
        context.execute("sendConfirmation", () -> logger.info("Booking confirmed!"));
      } catch (CompletionException e) {
        // CompletionException wraps StepFailedException when using synchronous execute
        if (e.getCause() instanceof StepFailedException stepFailed) {
          // 4. ALTERNATIVE PATH on error
          logger.warn("Payment failed: {}", stepFailed.getMessage());
          context.execute("cancelReservations", () -> {
            logger.info("Canceling flight {} and hotel {}", flightCode, hotelCode);
          });
        } else {
          throw e;
        }
      }
    }
  }

  @Test
  void shouldExecuteParallelStepsAndHandleFailureWithAlternativePath() {
    var context = engine.execute(new TravelBookingWorkflow(), Map.of("bookingId", "booking-001")).join();

    // Verify parallel steps ran
    assertThat(context.getStepHistory()).contains("reserveFlight", "reserveHotel");

    // Verify payment was attempted and alternative path taken
    assertThat(context.getStepHistory()).contains("processPayment", "cancelReservations");

    // Verify success path was NOT taken
    assertThat(context.getStepHistory()).doesNotContain("sendConfirmation");
  }
}
