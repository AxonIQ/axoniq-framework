package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple.SimpleContext;
import io.axoniq.workflow.dsl.simple.SimpleDefinition;
import io.axoniq.workflow.runtime.engine.Coordinator;
import io.axoniq.workflow.runtime.engine.SimpleStateManager;
import org.axonframework.common.infra.FilesystemStyleComponentDescriptor;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.axoniq.workflow.dsl.simple.Payload.payload;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class TravelBookingParallelTest {

  private static final Logger logger = LoggerFactory.getLogger(TravelBookingParallelTest.class);

  private SimpleStateManager stateManager;
  private Coordinator coordinator;
  private DelayedPublisher delayedPublisher;

  @BeforeEach
  void setUp() {
    stateManager = new SimpleStateManager();
    coordinator = new Coordinator(stateManager);
    delayedPublisher = new DelayedPublisher(stateManager);
  }

  @AfterEach
  void printEvents() {
    var descriptor = new FilesystemStyleComponentDescriptor();
    stateManager.describeTo(descriptor);
    logger.info(descriptor.describe());
  }

  static class TravelBookingWorkflow implements SimpleDefinition {

    private static void randomDelay(int minMs, int maxMs) {
      try {
        Thread.sleep(minMs + (int) (Math.random() * (maxMs - minMs)));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public String association(Map<String, Object> trigger) {
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

        //Lets process payment
        context.execute("processPayment", () -> {
          randomDelay(1000, 2000);
          throw new RuntimeException("Payment gateway unavailable");
        });

        // Success path
        // send confirmation to user
        context.execute("sendConfirmation", () -> logger.info("Booking confirmed!"));


      } catch (CompletionException e) {
          //CANT PROCESS PAYMENT ROLLBACK
          //ON ERROR PATH
          logger.warn("Payment failed: {}", e.getMessage());

          //Compensate! Rollback
          context.execute("cancelReservations",
            payload("flightCode", flightCode).with("hotelCode", hotelCode).getValues(),
            Boolean.class,
            payload -> {
              logger.info("Canceling flight {} and hotel {}", payload.get("flightCode"), payload.get("hotelCode"));
              return true;
            });


      }
    }
  }

  public record TravelRequired(
    String bookingId
  ) {

  }

  @Test
  void shouldExecuteParallelStepsAndHandleFailureWithAlternativePath() {

    coordinator.declarative().register(new QualifiedName(TravelRequired.class), new TravelBookingWorkflow());
    delayedPublisher.addSchedules(List.of(
      DelayedPublisher.Schedule.ofMillis(100, new TravelRequired("booking-001"))
    ));

    coordinator.start();
    delayedPublisher.start();

    await().untilAsserted(() -> {
      assertThat(coordinator.getHistory()).isNotEmpty();
    });

    var context = coordinator.getHistory().getFirst();
    assertThat(context).isNotNull();

    // Verify parallel steps ran
    assertThat(context.getStepHistory()).contains("reserveFlight", "reserveHotel");

    // Verify payment was attempted and alternative path taken
    assertThat(context.getStepHistory()).contains("processPayment", "cancelReservations");

    // Verify success path was NOT taken
    assertThat(context.getStepHistory()).doesNotContain("sendConfirmation");
  }

  @AfterEach
  void tearDown() {
    coordinator.stop();
  }
}
