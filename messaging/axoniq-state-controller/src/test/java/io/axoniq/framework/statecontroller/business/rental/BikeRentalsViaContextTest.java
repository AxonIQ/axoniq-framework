/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.statecontroller.business.rental;

import io.axoniq.framework.statecontroller.decisions.DecisionContext;
import org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.test.fixture.AxonTestFixture;
import org.junit.jupiter.api.*;

/**
 * Equivalence proof for the lazy {@link DecisionContext} read surface, written against Axon Framework's
 * {@link AxonTestFixture}. The Given-When-Then scenarios are identical to those of {@link BikeRentalsTest} — prior
 * events are seeded through the configured {@link EventSink} (where {@link AnnotationBasedTagResolver} auto-tags
 * them off the {@link org.axonframework.eventsourcing.annotation.EventTag @EventTag} on each event's
 * {@code bikeId} field), a command is dispatched, and the appended events / thrown exception are asserted. They
 * pass unchanged against the {@link BikeRentalsViaContext} decisions, demonstrating the lazy
 * {@code DecisionContext}/{@code EventStream} surface is behaviourally equivalent to the eager
 * {@code @Decide}/{@code History} surface exercised by {@link BikeRentalsTest}.
 * <p>
 * The fixture configures one annotated {@link BikeRentalsViaContext} component on top of an in-memory event
 * store, and lets the {@code @Decide} handler enhancer + {@code DecisionContext} parameter-resolver factory (both
 * ServiceLoader-discovered) wire the decision dispatch path automatically.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class BikeRentalsViaContextTest {

    private AxonTestFixture fixture;

    @BeforeEach
    void setUp() {
        var configurer = MessagingConfigurer.create().registerCommandHandlingModule(
                CommandHandlingModule.named("BikeRentalsViaContext")
                                     .commandHandlers(ch ->
                                                              ch.autodetectedCommandHandlingComponent(
                                                                      c -> new BikeRentalsViaContext()
                                                              )));
        fixture = AxonTestFixture.with(configurer);
    }

    @Nested
    class RentBikeFlow {

        @Test
        void anAvailableBikeYieldsAPendingRequest() {
            fixture.when()
                   .command(new RentBike("b1", "alice"))
                   .then()
                   .events(new BikeRequested("b1", "alice"));
        }

        @Test
        void rentingAnUnavailableBikeRejectsWithoutAppendingAnyEvents() {
            fixture.given()
                   .events(new BikeRequested("b1", "alice"),
                           new RequestApproved("b1", "alice"))
                   .when()
                   .command(new RentBike("b1", "bob"))
                   .then()
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("not available");
                   })
                   .noEvents();
        }

        @Test
        void aPreviouslyReturnedBikeCanBeRentedAgain() {
            fixture.given()
                   .events(new BikeRequested("b1", "alice"),
                           new RequestApproved("b1", "alice"),
                           new BikeReturned("b1", "alice"))
                   .when()
                   .command(new RentBike("b1", "bob"))
                   .then()
                   .events(new BikeRequested("b1", "bob"));
        }

        @Test
        void aRejectedRequestFreesTheBikeForRental() {
            fixture.given()
                   .events(new BikeRequested("b1", "alice"),
                           new RequestRejected("b1", "alice", "payment failed"))
                   .when()
                   .command(new RentBike("b1", "bob"))
                   .then()
                   .events(new BikeRequested("b1", "bob"));
        }

        @Test
        void aPendingRequestBlocksAnotherRental() {
            fixture.given()
                   .events(new BikeRequested("b1", "alice"))
                   .when()
                   .command(new RentBike("b1", "bob"))
                   .then()
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("not available");
                   })
                   .noEvents();
        }
    }

    @Nested
    class ReturnBikeFlow {

        @Test
        void theCurrentRenterCanReturnTheBike() {
            fixture.given()
                   .events(new BikeRequested("b1", "alice"),
                           new RequestApproved("b1", "alice"))
                   .when()
                   .command(new ReturnBike("b1", "alice"))
                   .then()
                   .events(new BikeReturned("b1", "alice"));
        }

        @Test
        void returningAPendingRequestIsRejectedBecauseNoApprovalLanded() {
            fixture.given()
                   .events(new BikeRequested("b1", "alice"))
                   .when()
                   .command(new ReturnBike("b1", "alice"))
                   .then()
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("not currently rented");
                   })
                   .noEvents();
        }

        @Test
        void returningAnUnrentedBikeIsRejected() {
            fixture.when()
                   .command(new ReturnBike("b1", "alice"))
                   .then()
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("not currently rented");
                   })
                   .noEvents();
        }

        @Test
        void returningABikeRentedByAnotherUserIsRejected() {
            fixture.given()
                   .events(new BikeRequested("b1", "alice"),
                           new RequestApproved("b1", "alice"))
                   .when()
                   .command(new ReturnBike("b1", "bob"))
                   .then()
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("rented by another user");
                   })
                   .noEvents();
        }

        @Test
        void returningABikeThatWasAlreadyReturnedIsRejected() {
            fixture.given()
                   .events(new BikeRequested("b1", "alice"),
                           new RequestApproved("b1", "alice"),
                           new BikeReturned("b1", "alice"))
                   .when()
                   .command(new ReturnBike("b1", "alice"))
                   .then()
                   .exceptionSatisfies(t -> {
                       assertThat(t).isInstanceOf(CommandExecutionException.class);
                       assertThat(t.getMessage()).contains("not currently rented");
                   });
        }
    }

    // Imported as a static method so the inline lambda assertions read cleanly.
    private static org.assertj.core.api.AbstractThrowableAssert<?, ? extends Throwable> assertThat(Throwable t) {
        return org.assertj.core.api.Assertions.assertThat(t);
    }

    private static org.assertj.core.api.AbstractStringAssert<?> assertThat(String s) {
        return org.assertj.core.api.Assertions.assertThat(s);
    }
}
