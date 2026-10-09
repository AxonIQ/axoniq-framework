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

package org.axonframework.test.saga;

import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.test.deadline.DeadlineManagerValidator;
import org.axonframework.test.deadline.StubDeadlineManager;
import org.axonframework.test.fixture.AxonTestPhase;
import org.axonframework.test.matchers.FieldFilter;
import org.axonframework.test.matchers.Matchers;
import org.hamcrest.Matcher;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.axonframework.test.matchers.Matchers.*;
import static org.hamcrest.CoreMatchers.any;

/**
 * Default implementation of {@link FixtureExecutionResult}, asserting against the {@link AxonTestPhase.Then then-phase}
 * the "when" phase produced.
 *
 * @author Allard Buijze
 * @author Mateusz Nowak
 * @since 5.4.0
 */
class FixtureExecutionResultImpl implements FixtureExecutionResult {

    private final Class<?> sagaType;
    private final AxonTestPhase.Then.Message<?> then;
    private final Runnable successfulHandlerExecution;
    private final FieldFilter fieldFilter;
    private final CommandValidator commandValidator;
    private final EventValidator eventValidator;
    private final DeadlineManagerValidator deadlineManagerValidator;

    /**
     * Constructs a {@code FixtureExecutionResultImpl} asserting on the then-phase of a published event.
     *
     * @param sagaType        the type of Saga under test, used to filter the store on the association assertions
     * @param then            the then-phase of the fixture the Saga was driven through
     * @param deadlineManager the deadline manager the Saga scheduled its deadlines on
     * @param fieldFilter     the filter describing the fields to include when comparing messages
     */
    FixtureExecutionResultImpl(Class<?> sagaType,
                               AxonTestPhase.Then.Event then,
                               StubDeadlineManager deadlineManager,
                               FieldFilter fieldFilter) {
        this(sagaType, then, then::success, deadlineManager, fieldFilter);
    }

    /**
     * Constructs a {@code FixtureExecutionResultImpl} asserting on the then-phase of moving the fixture's time.
     * <p>
     * A deadline handler that fails while time moves fails the "when" call itself, as it did in Axon Framework 4, so
     * by the time this result exists every handler executed successfully.
     *
     * @param sagaType        the type of Saga under test, used to filter the store on the association assertions
     * @param then            the then-phase of the fixture the Saga was driven through
     * @param deadlineManager the deadline manager the Saga scheduled its deadlines on
     * @param fieldFilter     the filter describing the fields to include when comparing messages
     */
    FixtureExecutionResultImpl(Class<?> sagaType,
                               AxonTestPhase.Then.Nothing then,
                               StubDeadlineManager deadlineManager,
                               FieldFilter fieldFilter) {
        this(sagaType, then, () -> {
        }, deadlineManager, fieldFilter);
    }

    private FixtureExecutionResultImpl(Class<?> sagaType,
                                       AxonTestPhase.Then.Message<?> then,
                                       Runnable successfulHandlerExecution,
                                       StubDeadlineManager deadlineManager,
                                       FieldFilter fieldFilter) {
        this.sagaType = Objects.requireNonNull(sagaType, "The sagaType may not be null.");
        this.then = Objects.requireNonNull(then, "The then-phase may not be null.");
        this.successfulHandlerExecution = successfulHandlerExecution;
        this.fieldFilter = Objects.requireNonNull(fieldFilter, "The fieldFilter may not be null.");
        this.commandValidator = new CommandValidator(this::dispatchedCommands, fieldFilter);
        this.eventValidator = new EventValidator(this::publishedEvents, fieldFilter);
        this.deadlineManagerValidator = new DeadlineManagerValidator(
                Objects.requireNonNull(deadlineManager, "The deadlineManager may not be null."), fieldFilter
        );
    }

    @Override
    public FixtureExecutionResult expectActiveSagas(int expected) {
        then.expect(SagaAssertions.activeSagas(expected));
        return this;
    }

    @Override
    public FixtureExecutionResult expectAssociationWith(String associationKey, Object associationValue) {
        then.expect(SagaAssertions.associationWith(sagaType, associationKey, associationValue));
        return this;
    }

    @Override
    public FixtureExecutionResult expectNoAssociationWith(String associationKey, Object associationValue) {
        then.expect(SagaAssertions.noAssociationWith(sagaType, associationKey, associationValue));
        return this;
    }

    @Override
    public FixtureExecutionResult expectDispatchedCommands(Object... commands) {
        commandValidator.assertDispatchedEqualTo(commands);
        return this;
    }

    @Override
    public FixtureExecutionResult expectDispatchedCommandsMatching(
            Matcher<? extends List<? super CommandMessage>> matcher
    ) {
        commandValidator.assertDispatchedMatching(matcher);
        return this;
    }

    @Override
    public FixtureExecutionResult expectNoDispatchedCommands() {
        commandValidator.assertDispatchedMatching(Matchers.noCommands());
        return this;
    }

    @Override
    public FixtureExecutionResult expectPublishedEvents(Object... expected) {
        eventValidator.assertPublishedEvents(expected);
        return this;
    }

    @Override
    public FixtureExecutionResult expectPublishedEventsMatching(
            Matcher<? extends List<? super EventMessage>> matcher
    ) {
        eventValidator.assertPublishedEventsMatching(matcher);
        return this;
    }

    @Override
    public FixtureExecutionResult expectSuccessfulHandlerExecution() {
        successfulHandlerExecution.run();
        return this;
    }


    @Override
    public FixtureExecutionResult expectScheduledEventMatching(Duration duration, Matcher<? super EventMessage> matcher) {
        // TODO #3104 - Axon Framework 4:
        // eventSchedulerValidator.assertScheduledEventMatching(duration, matcher);
        // return this;
        throw NotPorted.eventScheduler("expectScheduledEventMatching");
    }

    @Override
    public FixtureExecutionResult expectScheduledEvent(Duration duration, Object applicationEvent) {
        // TODO #3104 - Axon Framework 4:
        // return expectScheduledEventMatching(duration, messageWithPayload(deepEquals(applicationEvent, fieldFilter)));
        throw NotPorted.eventScheduler("expectScheduledEvent");
    }

    @Override
    public FixtureExecutionResult expectScheduledEventOfType(Duration duration, Class<?> eventType) {
        // TODO #3104 - Axon Framework 4:
        // return expectScheduledEventMatching(duration, messageWithPayload(any(eventType)));
        throw NotPorted.eventScheduler("expectScheduledEventOfType");
    }

    @Override
    public FixtureExecutionResult expectScheduledEventMatching(Instant scheduledTime, Matcher<? super EventMessage> matcher) {
        // TODO #3104 - Axon Framework 4:
        // eventSchedulerValidator.assertScheduledEventMatching(scheduledTime, matcher);
        // return this;
        throw NotPorted.eventScheduler("expectScheduledEventMatching");
    }

    @Override
    public FixtureExecutionResult expectScheduledEvent(Instant scheduledTime, Object applicationEvent) {
        // TODO #3104 - Axon Framework 4:
        // return expectScheduledEventMatching(scheduledTime,
        //                                     messageWithPayload(deepEquals(applicationEvent, fieldFilter)));
        throw NotPorted.eventScheduler("expectScheduledEvent");
    }

    @Override
    public FixtureExecutionResult expectScheduledEventOfType(Instant scheduledTime, Class<?> eventType) {
        // TODO #3104 - Axon Framework 4:
        // return expectScheduledEventMatching(scheduledTime, messageWithPayload(any(eventType)));
        throw NotPorted.eventScheduler("expectScheduledEventOfType");
    }

    @Override
    public FixtureExecutionResult expectNoScheduledEvents() {
        // TODO #3104 - Axon Framework 4:
        // eventSchedulerValidator.assertNoScheduledEvents();
        // return this;
        throw NotPorted.eventScheduler("expectNoScheduledEvents");
    }

    @Override
    public FixtureExecutionResult expectNoScheduledEventMatching(Duration durationToScheduledTime, Matcher<? super EventMessage> matcher) {
        // TODO #3104 - Axon Framework 4:
        // eventSchedulerValidator.assertNoScheduledEventMatching(durationToScheduledTime, matcher);
        // return this;
        throw NotPorted.eventScheduler("expectNoScheduledEventMatching");
    }

    @Override
    public FixtureExecutionResult expectNoScheduledEvent(Duration durationToScheduledTime, Object event) {
        // TODO #3104 - Axon Framework 4:
        // return expectNoScheduledEventMatching(durationToScheduledTime,
        //                                       messageWithPayload(deepEquals(event, fieldFilter)));
        throw NotPorted.eventScheduler("expectNoScheduledEvent");
    }

    @Override
    public FixtureExecutionResult expectNoScheduledEventOfType(Duration durationToScheduledTime, Class<?> eventType) {
        // TODO #3104 - Axon Framework 4:
        // return expectNoScheduledEventMatching(durationToScheduledTime, messageWithPayload(any(eventType)));
        throw NotPorted.eventScheduler("expectNoScheduledEventOfType");
    }

    @Override
    public FixtureExecutionResult expectNoScheduledEventMatching(Instant scheduledTime, Matcher<? super EventMessage> matcher) {
        // TODO #3104 - Axon Framework 4:
        // eventSchedulerValidator.assertNoScheduledEventMatching(scheduledTime, matcher);
        // return this;
        throw NotPorted.eventScheduler("expectNoScheduledEventMatching");
    }

    @Override
    public FixtureExecutionResult expectNoScheduledEvent(Instant scheduledTime, Object event) {
        // TODO #3104 - Axon Framework 4:
        // return expectNoScheduledEventMatching(scheduledTime, messageWithPayload(deepEquals(event, fieldFilter)));
        throw NotPorted.eventScheduler("expectNoScheduledEvent");
    }

    @Override
    public FixtureExecutionResult expectNoScheduledEventOfType(Instant scheduledTime, Class<?> eventType) {
        // TODO #3104 - Axon Framework 4:
        // return expectNoScheduledEventMatching(scheduledTime, messageWithPayload(any(eventType)));
        throw NotPorted.eventScheduler("expectNoScheduledEventOfType");
    }

    @Override
    public FixtureExecutionResult expectScheduledDeadlineMatching(Duration duration,
                                                                  Matcher<? super DeadlineMessage> matcher) {
        deadlineManagerValidator.assertScheduledDeadlineMatching(duration, matcher);
        return this;
    }

    @Override
    public FixtureExecutionResult expectScheduledDeadline(Duration duration, Object deadline) {
        return expectScheduledDeadlineMatching(duration, messageWithPayload(deepEquals(deadline, fieldFilter)));
    }

    @Override
    public FixtureExecutionResult expectScheduledDeadlineOfType(Duration duration, Class<?> deadlineType) {
        return expectScheduledDeadlineMatching(duration, messageWithPayload(any(deadlineType)));
    }

    @Override
    public FixtureExecutionResult expectScheduledDeadlineWithName(Duration duration, String deadlineName) {
        return expectScheduledDeadlineMatching(
                duration,
                matches(deadlineMessage -> deadlineMessage.getDeadlineName().equals(deadlineName))
        );
    }

    @Override
    public FixtureExecutionResult expectScheduledDeadlineMatching(Instant scheduledTime,
                                                                  Matcher<? super DeadlineMessage> matcher) {
        deadlineManagerValidator.assertScheduledDeadlineMatching(scheduledTime, matcher);
        return this;
    }

    @Override
    public FixtureExecutionResult expectScheduledDeadline(Instant scheduledTime, Object deadline) {
        return expectScheduledDeadlineMatching(scheduledTime, messageWithPayload(deepEquals(deadline, fieldFilter)));
    }

    @Override
    public FixtureExecutionResult expectScheduledDeadlineOfType(Instant scheduledTime, Class<?> deadlineType) {
        return expectScheduledDeadlineMatching(scheduledTime, messageWithPayload(any(deadlineType)));
    }

    @Override
    public FixtureExecutionResult expectScheduledDeadlineWithName(Instant scheduledTime, String deadlineName) {
        return expectScheduledDeadlineMatching(
                scheduledTime,
                matches(deadlineMessage -> deadlineMessage.getDeadlineName().equals(deadlineName))
        );
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadlines() {
        deadlineManagerValidator.assertNoScheduledDeadlines();
        return this;
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadlineMatching(Matcher<? super DeadlineMessage> matcher) {
        deadlineManagerValidator.assertNoScheduledDeadlineMatching(matcher);
        return this;
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadlineMatching(Duration durationToScheduledTime,
                                                                    Matcher<? super DeadlineMessage> matcher) {
        Instant scheduledTime = deadlineManagerValidator.currentDateTime().plus(durationToScheduledTime);
        return expectNoScheduledDeadlineMatching(scheduledTime, matcher);
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadline(Duration durationToScheduledTime, Object deadline) {
        return expectNoScheduledDeadlineMatching(durationToScheduledTime,
                                                 messageWithPayload(deepEquals(deadline, fieldFilter)));
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadlineOfType(Duration durationToScheduledTime,
                                                                  Class<?> deadlineType) {
        return expectNoScheduledDeadlineMatching(durationToScheduledTime, messageWithPayload(any(deadlineType)));
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadlineWithName(Duration durationToScheduledTime,
                                                                    String deadlineName) {
        return expectNoScheduledDeadlineMatching(
                durationToScheduledTime,
                matches(deadlineMessage -> deadlineMessage.getDeadlineName().equals(deadlineName))
        );
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadlineMatching(Instant scheduledTime,
                                                                    Matcher<? super DeadlineMessage> matcher) {
        return expectNoScheduledDeadlineMatching(matches(
                deadlineMessage -> deadlineMessage.timestamp().equals(scheduledTime)
                        && matcher.matches(deadlineMessage)
        ));
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadline(Instant scheduledTime, Object deadline) {
        return expectNoScheduledDeadlineMatching(scheduledTime, messageWithPayload(deepEquals(deadline, fieldFilter)));
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadlineOfType(Instant scheduledTime, Class<?> deadlineType) {
        return expectNoScheduledDeadlineMatching(scheduledTime, messageWithPayload(any(deadlineType)));
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadlineWithName(Instant scheduledTime, String deadlineName) {
        return expectNoScheduledDeadlineMatching(
                scheduledTime,
                matches(deadlineMessage -> deadlineMessage.getDeadlineName().equals(deadlineName))
        );
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadlineMatching(Instant from,
                                                                    Instant to,
                                                                    Matcher<? super DeadlineMessage> matcher) {
        return expectNoScheduledDeadlineMatching(matches(
                deadlineMessage -> !(deadlineMessage.timestamp().isBefore(from)
                        || deadlineMessage.timestamp().isAfter(to))
                        && matcher.matches(deadlineMessage)
        ));
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadline(Instant from, Instant to, Object deadline) {
        return expectNoScheduledDeadlineMatching(from, to, messageWithPayload(deepEquals(deadline, fieldFilter)));
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadlineOfType(Instant from, Instant to, Class<?> deadlineType) {
        return expectNoScheduledDeadlineMatching(from, to, messageWithPayload(any(deadlineType)));
    }

    @Override
    public FixtureExecutionResult expectNoScheduledDeadlineWithName(Instant from, Instant to, String deadlineName) {
        return expectNoScheduledDeadlineMatching(
                from, to, matches(deadlineMessage -> deadlineMessage.getDeadlineName().equals(deadlineName))
        );
    }

    @Override
    public FixtureExecutionResult expectDeadlinesMetMatching(
            Matcher<? extends List<?>> matcher
    ) {
        return expectTriggeredDeadlinesMatching(matcher);
    }

    @Override
    public FixtureExecutionResult expectTriggeredDeadlinesMatching(
            Matcher<? extends List<?>> matcher
    ) {
        deadlineManagerValidator.assertTriggeredDeadlinesMatching(matcher);
        return this;
    }

    @Override
    public FixtureExecutionResult expectDeadlinesMet(Object... expected) {
        return expectTriggeredDeadlines(expected);
    }

    @Override
    public FixtureExecutionResult expectTriggeredDeadlines(Object... expected) {
        deadlineManagerValidator.assertTriggeredDeadlines(expected);
        return this;
    }

    @Override
    public FixtureExecutionResult expectTriggeredDeadlinesWithName(String... expectedDeadlineNames) {
        deadlineManagerValidator.assertTriggeredDeadlinesWithName(expectedDeadlineNames);
        return this;
    }

    @Override
    public FixtureExecutionResult expectTriggeredDeadlinesOfType(Class<?>... expectedDeadlineTypes) {
        deadlineManagerValidator.assertTriggeredDeadlinesOfType(expectedDeadlineTypes);
        return this;
    }

    private List<CommandMessage> dispatchedCommands() {
        AtomicReference<List<CommandMessage>> captured = new AtomicReference<>();
        then.commandsSatisfy(captured::set);
        return captured.get();
    }

    private List<EventMessage> publishedEvents() {
        AtomicReference<List<EventMessage>> captured = new AtomicReference<>();
        then.eventsSatisfy(captured::set);
        return captured.get();
    }
}
