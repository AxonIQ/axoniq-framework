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
     * <p>
     * Can be chained to build natural sentences: {@code andThenAggregate(someIdentifier).published(someEvents)}
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
     */
    ContinuedGivenState andThenTimeElapses(Duration elapsedTime);

    /**
     * Simulate time shifts in the current given state. This can be useful when the time between given events is of
     * importance.
     *
     * @param newDateTime The time to advance the clock to
     * @return an object that allows registration of the actual events to send
     */
    ContinuedGivenState andThenTimeAdvancesTo(Instant newDateTime);

    /**
     * Indicates that the given {@code event} has been published in the past. This event is sent to the associated
     * sagas.
     *
     * @param event The event to publish
     * @return an object that allows chaining of more given state
     */
    ContinuedGivenState andThenAPublished(Object event);

    /**
     * Indicates that the given {@code event} with given {@code metadata} has been published in the past. This event is sent to the associated
     * sagas.
     *
     * @param event The event to publish
     * @param metadata The metadata to attach to the event
     * @return an object that allows chaining of more given state
     */
    ContinuedGivenState andThenAPublished(Object event, Map<String, String> metadata);
}
