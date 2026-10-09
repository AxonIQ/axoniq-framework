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
import org.axonframework.test.AxonAssertionError;
import org.hamcrest.Matcher;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Interface towards an object that contains the results of a Saga test fixture execution. Assertions are made against
 * the state of the Sagas and the messages they produced during the "when" phase.
 *
 * @author Allard Buijze
 * @since 1.1
 */
public interface FixtureExecutionResult {

    /**
     * Asserts that the repository contains the given {@code expected} amount of active Sagas.
     * <p>
     * Counts every Saga in the store, whatever its type, as Axon Framework 4 did. That makes this assertion asymmetric
     * with {@link #expectAssociationWith(String, Object)}, which does filter on the Saga type under test.
     *
     * @param expected the expected number of active Sagas in this fixture
     * @return the FixtureExecutionResult for method chaining
     * @throws AxonAssertionError when the store holds another number of Sagas
     */
    FixtureExecutionResult expectActiveSagas(int expected);

    /**
     * Asserts that at least one of the active Sagas is associated with the given {@code associationKey} and
     * {@code associationValue}.
     * <p>
     * The {@code associationValue} is compared by its {@link Object#toString() string representation}, as Axon
     * Framework 4 did.
     *
     * @param associationKey   the key of the association
     * @param associationValue the value of the association
     * @return the FixtureExecutionResult for method chaining
     * @throws AxonAssertionError when no Saga holds the association
     */
    FixtureExecutionResult expectAssociationWith(String associationKey, Object associationValue);

    /**
     * Asserts that none of the active Sagas is associated with the given {@code associationKey} and
     * {@code associationValue}.
     * <p>
     * The {@code associationValue} is compared by its {@link Object#toString() string representation}, as Axon
     * Framework 4 did.
     *
     * @param associationKey   the key of the association
     * @param associationValue the value of the association
     * @return the FixtureExecutionResult for method chaining
     * @throws AxonAssertionError when a Saga holds the association
     */
    FixtureExecutionResult expectNoAssociationWith(String associationKey, Object associationValue);

    /**
     * Asserts that the Sagas dispatched the given commands, in the exact sequence given.
     * <p>
     * Each element is either a {@link CommandMessage}, in which case payload and metadata are both compared, or a
     * payload, in which case only the payload is compared.
     *
     * @param commands the commands expected to have been dispatched
     * @return the FixtureExecutionResult for method chaining
     * @throws AxonAssertionError when another set of commands was dispatched
     */
    FixtureExecutionResult expectDispatchedCommands(Object... commands);

    /**
     * Asserts that the Sagas dispatched commands matching the given {@code matcher}.
     *
     * @param matcher the matcher validating the dispatched commands
     * @return the FixtureExecutionResult for method chaining
     * @throws AxonAssertionError when the dispatched commands do not match
     */
    FixtureExecutionResult expectDispatchedCommandsMatching(Matcher<? extends List<? super CommandMessage>> matcher);

    /**
     * Asserts that the Sagas dispatched no commands.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws AxonAssertionError when any command was dispatched
     */
    FixtureExecutionResult expectNoDispatchedCommands();

    /**
     * Asserts that the Sagas published the given events, in the exact sequence given.
     * <p>
     * Each element is either an {@link EventMessage} or a payload; a message is unwrapped to its payload, so only
     * payloads are compared and metadata is not, as in Axon Framework 4. The event that drove the "when" phase is not
     * part of this set, since the test published it rather than the Saga.
     *
     * @param expected the events expected to have been published
     * @return the FixtureExecutionResult for method chaining
     * @throws AxonAssertionError when another set of events was published
     */
    FixtureExecutionResult expectPublishedEvents(Object... expected);

    /**
     * Asserts that the Sagas published events matching the given {@code matcher}.
     * <p>
     * The event that drove the "when" phase is not part of the set the matcher sees, since the test published it
     * rather than the Saga.
     *
     * @param matcher the matcher validating the published events
     * @return the FixtureExecutionResult for method chaining
     * @throws AxonAssertionError when the published events do not match
     */
    FixtureExecutionResult expectPublishedEventsMatching(Matcher<? extends List<? super EventMessage>> matcher);

    /**
     * Asserts that the Saga handled the "when" event without failing.
     * <p>
     * Unlike Axon Framework 4, a failing {@code @SagaEventHandler} propagates by default rather than being logged and
     * swallowed, so this assertion holds unless the Saga threw. A Saga that suppresses its own failures with an
     * {@code @ExceptionHandler} passes it, as it did in Axon Framework 4.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws AxonAssertionError when handling the "when" event failed
     */
    FixtureExecutionResult expectSuccessfulHandlerExecution();

    // TODO #3104 - the event scheduler has not been ported into axoniq-legacy. The scheduled event assertions below are
    // declared so an Axon Framework 4 test suite still compiles, but every call throws an
    // UnsupportedOperationException instead of passing without exercising the requested behaviour. The Axon
    // Framework 4 implementation of each is kept as a comment in FixtureExecutionResultImpl.

    /**
     * Asserts that an event matching the given {@code matcher} is scheduled after the given {@code duration}.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws UnsupportedOperationException always, as {@code axoniq-legacy} carries no event scheduler
     */
    FixtureExecutionResult expectScheduledEventMatching(Duration duration, Matcher<? super EventMessage> matcher);

    /**
     * Asserts that the given {@code applicationEvent} is scheduled after the given {@code duration}.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws UnsupportedOperationException always, as {@code axoniq-legacy} carries no event scheduler
     */
    FixtureExecutionResult expectScheduledEvent(Duration duration, Object applicationEvent);

    /**
     * Asserts that an event of the given {@code eventType} is scheduled after the given {@code duration}.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws UnsupportedOperationException always, as {@code axoniq-legacy} carries no event scheduler
     */
    FixtureExecutionResult expectScheduledEventOfType(Duration duration, Class<?> eventType);

    /**
     * Asserts that an event matching the given {@code matcher} is scheduled at the given {@code scheduledTime}.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws UnsupportedOperationException always, as {@code axoniq-legacy} carries no event scheduler
     */
    FixtureExecutionResult expectScheduledEventMatching(Instant scheduledTime, Matcher<? super EventMessage> matcher);

    /**
     * Asserts that the given {@code applicationEvent} is scheduled at the given {@code scheduledTime}.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws UnsupportedOperationException always, as {@code axoniq-legacy} carries no event scheduler
     */
    FixtureExecutionResult expectScheduledEvent(Instant scheduledTime, Object applicationEvent);

    /**
     * Asserts that an event of the given {@code eventType} is scheduled at the given {@code scheduledTime}.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws UnsupportedOperationException always, as {@code axoniq-legacy} carries no event scheduler
     */
    FixtureExecutionResult expectScheduledEventOfType(Instant scheduledTime, Class<?> eventType);

    /**
     * Asserts that no events are scheduled.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws UnsupportedOperationException always, as {@code axoniq-legacy} carries no event scheduler
     */
    FixtureExecutionResult expectNoScheduledEvents();

    /**
     * Asserts that no event matching the given {@code matcher} is scheduled after the given duration.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws UnsupportedOperationException always, as {@code axoniq-legacy} carries no event scheduler
     */
    FixtureExecutionResult expectNoScheduledEventMatching(Duration durationToScheduledTime, Matcher<? super EventMessage> matcher);

    /**
     * Asserts that the given {@code event} is not scheduled after the given duration.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws UnsupportedOperationException always, as {@code axoniq-legacy} carries no event scheduler
     */
    FixtureExecutionResult expectNoScheduledEvent(Duration durationToScheduledTime, Object event);

    /**
     * Asserts that no event of the given {@code eventType} is scheduled after the given duration.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws UnsupportedOperationException always, as {@code axoniq-legacy} carries no event scheduler
     */
    FixtureExecutionResult expectNoScheduledEventOfType(Duration durationToScheduledTime, Class<?> eventType);

    /**
     * Asserts that no event matching the given {@code matcher} is scheduled at the given {@code scheduledTime}.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws UnsupportedOperationException always, as {@code axoniq-legacy} carries no event scheduler
     */
    FixtureExecutionResult expectNoScheduledEventMatching(Instant scheduledTime, Matcher<? super EventMessage> matcher);

    /**
     * Asserts that the given {@code event} is not scheduled at the given {@code scheduledTime}.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws UnsupportedOperationException always, as {@code axoniq-legacy} carries no event scheduler
     */
    FixtureExecutionResult expectNoScheduledEvent(Instant scheduledTime, Object event);

    /**
     * Asserts that no event of the given {@code eventType} is scheduled at the given {@code scheduledTime}.
     *
     * @return the FixtureExecutionResult for method chaining
     * @throws UnsupportedOperationException always, as {@code axoniq-legacy} carries no event scheduler
     */
    FixtureExecutionResult expectNoScheduledEventOfType(Instant scheduledTime, Class<?> eventType);

    /**
     * Asserts that a deadline scheduled after given {@code duration} matches the given {@code matcher}.
     *
     * @param duration the delay expected before the deadline is met
     * @param matcher  the matcher that must match with the deadline scheduled at the given time
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectScheduledDeadlineMatching(Duration duration,
                                                           Matcher<? super DeadlineMessage> matcher);

    /**
     * Asserts that a deadline equal to the given {@code deadline} has been scheduled after the given {@code duration}.
     * <p/>
     * Note that the source attribute of the deadline is ignored when comparing deadlines. Deadlines are compared using
     * an "equals" check on all fields in the deadlines.
     *
     * @param duration the time to wait before the deadline should be met
     * @param deadline the expected deadline
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectScheduledDeadline(Duration duration, Object deadline);

    /**
     * Asserts that a deadline of the given {@code deadlineType} has been scheduled after the given {@code duration}.
     *
     * @param duration     the time to wait before the deadline is met
     * @param deadlineType the type of the expected deadline
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectScheduledDeadlineOfType(Duration duration, Class<?> deadlineType);

    /**
     * Asserts that a deadline with the given {@code deadlineName} has been scheduled after the given {@code duration}.
     *
     * @param duration     the time to wait before the deadline is met
     * @param deadlineName the name of the expected deadline
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectScheduledDeadlineWithName(Duration duration, String deadlineName);

    /**
     * Asserts that a deadline matching the given {@code matcher} has been scheduled at the given {@code
     * scheduledTime}.
     * <p/>
     * If the {@code scheduledTime} is calculated based on the "current time", use the {@link
     * FixtureConfiguration#currentTime()} to get the time to use as "current time".
     *
     * @param scheduledTime the time at which the deadline should be met
     * @param matcher       the matcher defining the deadline expected
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectScheduledDeadlineMatching(Instant scheduledTime,
                                                           Matcher<? super DeadlineMessage> matcher);

    /**
     * Asserts that a deadline equal to the given {@code deadline} has been scheduled at the given {@code
     * scheduledTime}.
     * <p/>
     * If the {@code scheduledTime} is calculated based on the "current time", use the {@link
     * FixtureConfiguration#currentTime()} to get the time to use as "current time".
     * <p/>
     * Note that the source attribute of the deadline is ignored when comparing deadlines. Deadlines are compared using
     * an "equals" check on all fields in the deadlines.
     *
     * @param scheduledTime the time at which the deadline is scheduled
     * @param deadline      the expected deadline
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectScheduledDeadline(Instant scheduledTime, Object deadline);

    /**
     * Asserts that a deadline of the given {@code deadlineType} has been scheduled at the given {@code scheduledTime}.
     *
     * @param scheduledTime the time at which the deadline is scheduled
     * @param deadlineType  the type of the expected deadline
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectScheduledDeadlineOfType(Instant scheduledTime, Class<?> deadlineType);

    /**
     * Asserts that a deadline with the given {@code deadlineName} has been scheduled at the given {@code
     * scheduledTime}.
     *
     * @param scheduledTime the time at which the deadline is scheduled
     * @param deadlineName  the name of the expected deadline
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectScheduledDeadlineWithName(Instant scheduledTime, String deadlineName);

    /**
     * Asserts that no deadlines are scheduled. This means that either no deadlines were scheduled at all, all schedules
     * have been cancelled or all scheduled deadlines have been met already.
     *
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectNoScheduledDeadlines();

    /**
     * Asserts that <b>no</b> deadline matching the given {@code matcher} is scheduled. Can be used to validate if a
     * deadline has never been set or has been canceled.
     *
     * @param matcher the matcher defining the deadline which should not be scheduled
     * @return the current ResultValidator, for fluent interfacing
     */
    FixtureExecutionResult expectNoScheduledDeadlineMatching(Matcher<? super DeadlineMessage> matcher);

    /**
     * Asserts that <b>no</b> deadline matching the given {@code matcher} should be scheduled after the given {@code
     * durationToScheduledTime}. Can be used to validate if a deadline has never been set or has been canceled at an
     * <b>exact</b> moment in time.
     *
     * @param durationToScheduledTime the time to wait until the trigger point of the deadline which should not be
     *                                scheduled
     * @param matcher                 the matcher defining the deadline which should not be scheduled
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectNoScheduledDeadlineMatching(Duration durationToScheduledTime,
                                                             Matcher<? super DeadlineMessage> matcher);

    /**
     * Asserts that <b>no</b> deadline equal to the given {@code deadline} has been scheduled after the given {@code
     * durationToScheduledTime}. Can be used to validate if a deadline has never been set or has been canceled at an
     * <b>exact</b> moment in time.
     * <p/>
     * Note that the source attribute of the deadline is ignored when comparing deadlines. Deadlines are compared using
     * an "equals" check on all fields in the deadlines.
     *
     * @param durationToScheduledTime the time to wait until the trigger point of the deadline which should not be
     *                                scheduled
     * @param deadline                the deadline which should not be scheduled
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectNoScheduledDeadline(Duration durationToScheduledTime, Object deadline);

    /**
     * Asserts that <b>no</b> deadline of the given {@code deadlineType} has been scheduled at the given {@code
     * durationToScheduledTime}. Can be used to validate if a deadline has never been set or has been canceled at an
     * <b>exact</b> moment in time.
     *
     * @param durationToScheduledTime the time to wait until the trigger point of the deadline which should not be
     *                                scheduled
     * @param deadlineType            the type of the deadline which should not be scheduled
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectNoScheduledDeadlineOfType(Duration durationToScheduledTime, Class<?> deadlineType);

    /**
     * Asserts that <b>no</b> deadline with the given {@code deadlineName} has been scheduled after the given {@code
     * durationToScheduledTime}. Can be used to validate if a deadline has never been set or has been canceled at an
     * <b>exact</b> moment in time.
     *
     * @param durationToScheduledTime the time to wait until the trigger point of the deadline which should not be
     *                                scheduled
     * @param deadlineName            the name of the deadline which should not be scheduled
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectNoScheduledDeadlineWithName(Duration durationToScheduledTime, String deadlineName);

    /**
     * Asserts that <b>no</b> deadline matching the given {@code matcher} has been scheduled at the given {@code
     * scheduledTime}. Can be used to validate if a deadline has never been set or has been canceled at an exact moment
     * in time.
     * <p/>
     * If the {@code scheduledTime} is calculated based on the "current time", use the {@link
     * TestExecutor#currentTime()} to get the time to use as "current time".
     *
     * @param scheduledTime the time at which no deadline matching the given {@code matcher} should be scheduled
     * @param matcher       the matcher defining the deadline which should not be scheduled
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectNoScheduledDeadlineMatching(Instant scheduledTime,
                                                             Matcher<? super DeadlineMessage> matcher);

    /**
     * Asserts that <b>no</b> deadline equal to the given {@code deadline} has been scheduled at the given {@code
     * scheduledTime}. Can be used to validate if a deadline has never been set or has been canceled at an exact moment
     * in time.
     * <p/>
     * If the {@code scheduledTime} is calculated based on the "current time", use the {@link
     * TestExecutor#currentTime()} to get the time to use as "current time".
     * <p/>
     * Note that the source attribute of the deadline is ignored when comparing deadlines. Deadlines are compared using
     * an "equals" check on all fields in the deadlines.
     *
     * @param scheduledTime the time at which no deadline equal to the given {@code deadline} should be scheduled
     * @param deadline      the deadline which should not be scheduled
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectNoScheduledDeadline(Instant scheduledTime, Object deadline);

    /**
     * Asserts that <b>no</b> deadline with the given {@code deadlineType} has been scheduled at the given {@code
     * scheduledTime}. Can be used to validate if a deadline has never been set or has been canceled at an exact moment
     * in time.
     *
     * @param scheduledTime the time at which no deadline of {@code deadlineType} should be scheduled
     * @param deadlineType  the type of the deadline which should not be scheduled
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectNoScheduledDeadlineOfType(Instant scheduledTime, Class<?> deadlineType);

    /**
     * Asserts that <b>no</b> deadline with the given {@code deadlineName} has been scheduled at the given {@code
     * scheduledTime}. Can be used to validate if a deadline has never been set or has been canceled at an exact moment
     * in time.
     *
     * @param scheduledTime the time at which no deadline of {@code deadlineName} should be scheduled
     * @param deadlineName  the name of the deadline which should not be scheduled
     * @return the FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectNoScheduledDeadlineWithName(Instant scheduledTime, String deadlineName);

    /**
     * Asserts that <b>no</b> deadline matching the given {@code matcher} has been scheduled between the {@code to} and
     * {@code from} times, where {@code to} and {@code from} are inclusive. Can be used to validate if a deadline has
     * never been set or has been canceled within a given timeframe.
     *
     * @param from    the time from which no deadline equal to the given {@code deadline} should be scheduled
     *                (inclusive)
     * @param to      the time until which no deadline equal to the given {@code deadline} should be scheduled
     *                (inclusive)
     * @param matcher the matcher defining the deadline which should not be scheduled
     * @return the current ResultValidator, for fluent interfacing
     */
    FixtureExecutionResult expectNoScheduledDeadlineMatching(Instant from, Instant to,
                                                             Matcher<? super DeadlineMessage> matcher);

    /**
     * Asserts that <b>no</b> deadline equal to the given {@code deadline} has been scheduled between the {@code to} and
     * {@code from} times, where {@code to} and {@code from} are inclusive. Can be used to validate if a deadline has
     * never been set or has been canceled within a given timeframe.
     *
     * @param from     the time from which no deadline equal to the given {@code deadline} should be scheduled
     *                 (inclusive)
     * @param to       the time until which no deadline equal to the given {@code deadline} should be scheduled
     *                 (inclusive)
     * @param deadline the deadline which should not be scheduled
     * @return the current ResultValidator, for fluent interfacing
     */
    FixtureExecutionResult expectNoScheduledDeadline(Instant from, Instant to, Object deadline);

    /**
     * Asserts that <b>no</b> deadline with the given {@code deadlineType} has been scheduled between the {@code to} and
     * {@code from} times, where {@code to} and {@code from} are inclusive. Can be used to validate if a deadline has
     * never been set or has been canceled within a given timeframe.
     *
     * @param from         the time from which no deadline equal to the given {@code deadline} should be scheduled
     *                     (inclusive)
     * @param to           the time until which no deadline equal to the given {@code deadline} should be scheduled
     *                     (inclusive)
     * @param deadlineType the type of the deadline which should not be scheduled
     * @return the current ResultValidator, for fluent interfacing
     */
    FixtureExecutionResult expectNoScheduledDeadlineOfType(Instant from, Instant to, Class<?> deadlineType);

    /**
     * Asserts that <b>no</b> deadline with the given {@code deadlineName} has been scheduled between the {@code to} and
     * {@code from} times, where {@code to} and {@code from} are inclusive. Can be used to validate if a deadline has
     * never been set or has been canceled within a given timeframe.
     *
     * @param from         the time from which no deadline equal to the given {@code deadline} should be scheduled
     *                     (inclusive)
     * @param to           the time until which no deadline equal to the given {@code deadline} should be scheduled
     *                     (inclusive)
     * @param deadlineName the name of the deadline which should not be scheduled
     * @return the current ResultValidator, for fluent interfacing
     */
    FixtureExecutionResult expectNoScheduledDeadlineWithName(Instant from, Instant to, String deadlineName);

    /**
     * Asserts that deadlines match given {@code matcher} have been met (which have passed in time) on this saga.
     *
     * @param matcher the matcher that defines the expected list of deadlines
     * @return the FixtureExecutionResult for method chaining
     * @deprecated in favor of {@link #expectTriggeredDeadlinesMatching(Matcher)}
     */
    @Deprecated
    FixtureExecutionResult expectDeadlinesMetMatching(Matcher<? extends List<?>> matcher);

    /**
     * Asserts that deadlines matching the given {@code matcher} have been triggered for this aggregate.
     * <p>
     * The {@code matcher} is matched against the list of triggered {@link DeadlineMessage DeadlineMessages}. Axon
     * Framework 4 typed it as a matcher of a list of a super type of {@code DeadlineMessage}, which Axon Framework 5's
     * {@link org.axonframework.test.matchers.Matchers#payloadsMatching(Matcher)} no longer satisfies, as it matches a
     * list of a subtype of {@code Message}. The parameter therefore accepts a matcher of any list, so both compile.
     *
     * @param matcher the matcher that defines the expected list of deadlines
     * @return the current FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectTriggeredDeadlinesMatching(Matcher<? extends List<?>> matcher);

    /**
     * Asserts that given {@code expected} deadlines have been met (which have passed in time). Deadlines are compared
     * comparing their type and fields using "equals".
     *
     * @param expected the sequence of deadlines expected to be met
     * @return the FixtureExecutionResult for method chaining
     * @deprecated in favor of {@link #expectTriggeredDeadlines(Object...)}
     */
    @Deprecated
    FixtureExecutionResult expectDeadlinesMet(Object... expected);

    /**
     * Asserts that given {@code expected} deadlines have been triggered. Deadlines are compared comparing their type
     * and fields using "equals".
     *
     * @param expected the sequence of deadlines expected to have been triggered
     * @return the current FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectTriggeredDeadlines(Object... expected);

    /**
     * Asserts that the given {@code expectedDeadlineNames} have been triggered. Matches that the given names are
     * complete, in the same order and match the triggered deadlines by validating with {@link
     * DeadlineMessage#getDeadlineName()}.
     *
     * @param expectedDeadlineNames the sequence of deadline names expected to have been triggered
     * @return the current FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectTriggeredDeadlinesWithName(String... expectedDeadlineNames);

    /**
     * Asserts that the given {@code expectedDeadlineTypes} have been triggered. Matches that the given types are
     * complete, in the same order and match the triggered deadlines by validating with {@link
     * DeadlineMessage#getPayloadType()}.
     *
     * @param expectedDeadlineTypes the sequence of deadline types expected to have been triggered
     * @return the current FixtureExecutionResult for method chaining
     */
    FixtureExecutionResult expectTriggeredDeadlinesOfType(Class<?>... expectedDeadlineTypes);
}
