/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.test.saga;


import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Interface describing methods that can be executed after the first "given" state has been supplied. Either more
 * "given" state can be appended, or a transition to the definition of "when" can be made.
 *
 * @author Allard Buijze
 * @since 1.1
 */
public interface ContinuedGivenState extends WhenState {

    /**
     * Use this method to indicate that an aggregate with given identifier published certain events.
     * <p/>
     * Can be chained to build natural sentences:<br/> {@code andThenAggregate(someIdentifier).published(someEvents)
     * }
     *
     * @param aggregateIdentifier The identifier of the aggregate the events should appear to come from
     * @return an object that allows registration of the actual events to send
     */
    GivenAggregateEventPublisher andThenAggregate(String aggregateIdentifier);

    /**
     * Simulate time shifts in the current given state. This can be useful when the time between given events is of
     * importance.
     *
     * @param elapsedTime The amount of time that will elapse
     * @return an object that allows registration of the actual events to send
     * @throws Exception if an exception happens when the duration elapses
     */
    ContinuedGivenState andThenTimeElapses(Duration elapsedTime) throws Exception;

    /**
     * Simulate time shifts in the current given state. This can be useful when the time between given events is of
     * importance.
     *
     * @param newDateTime The time to advance the clock to
     * @return an object that allows registration of the actual events to send
     * @throws Exception if an exception happens when the time advances
     */
    ContinuedGivenState andThenTimeAdvancesTo(Instant newDateTime) throws Exception;

    /**
     * Indicates that the given {@code event} has been published in the past. This event is sent to the associated
     * sagas.
     *
     * @param event The event to publish
     * @return an object that allows chaining of more given state
     * @throws Exception if an exception happens when the event is handled
     */
    ContinuedGivenState andThenAPublished(Object event);

    /**
     * Indicates that the given {@code event} with given {@code metadata} has been published in the past. This event is sent to the associated
     * sagas.
     *
     * @param event The event to publish
     * @param metadata The metadata to attach to the event
     * @return an object that allows chaining of more given state
     * @throws Exception if an exception happens when the event is handled
     */
    ContinuedGivenState andThenAPublished(Object event, Map<String, String> metadata);
}
