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
 * Interface providing an API to methods in the "when" state of the fixture execution. Unlike the methods in the
 * "given" state, these methods record the behavior of the Sagas involved for validation.
 *
 * @author Allard Buijze
 * @since 2.1.1
 */
public interface WhenState {

    /**
     * Use this method to indicate that an aggregate with given identifier should publish certain events, <em>while
     * recording the outcome</em>. In contrast to the {@link FixtureConfiguration#givenAggregate(String) given} and
     * {@link org.axonframework.test.saga.ContinuedGivenState#andThenAggregate(String) andThen} methods, this method
     * will start recording activity on the EventBus and CommandBus.
     * <p/>
     * Can be chained to build natural sentences:<br/> {@code whenAggregate(someIdentifier).publishes(anEvent)}
     * <p/>
     * Note that if you inject resources using {@link FixtureConfiguration#registerResource(Object)}, you may need to
     * reset them yourself if they are manipulated by the Saga in the "given" stage of the test.
     *
     * @param aggregateIdentifier The identifier of the aggregate the events should appear to come from
     * @return an object that allows registration of the actual events to send
     */
    WhenAggregateEventPublisher whenAggregate(String aggregateIdentifier);

    /**
     * Use this method to indicate an application is published, <em>while recording the outcome</em>.
     * <p/>
     * Note that if you inject resources using {@link FixtureConfiguration#registerResource(Object)}, you may need to
     * reset them yourself if they are manipulated by the Saga in the "given" stage of the test.
     *
     * @param event the event to publish
     * @return an object allowing you to verify the test results
     */
    FixtureExecutionResult whenPublishingA(Object event);

    /**
     * Use this method to indicate an application is published with given additional {@code metadata},
     * <em>while recording the outcome</em>.
     * <p/>
     * Note that if you inject resources using {@link FixtureConfiguration#registerResource(Object)}, you may need to
     * reset them yourself if they are manipulated by the Saga in the "given" stage of the test.
     *
     * @param event the event to publish
     * @param metadata The metadata to attach to the event
     * @return an object allowing you to verify the test results
     */
    FixtureExecutionResult whenPublishingA(Object event, Map<String, String> metadata);

    /**
     * Mimic an elapsed time with no relevant activity for the Saga. If any Events are scheduled to be published within
     * this time frame, they are published. All activity by the Saga on the CommandBus and EventBus (meaning that
     * scheduled events are excluded) is recorded.
     * <p/>
     * Note that if you inject resources using {@link FixtureConfiguration#registerResource(Object)}, you may need to
     * reset them yourself if they are manipulated by the Saga in the "given" stage of the test.
     *
     * @param elapsedTime The amount of time to elapse
     * @return an object allowing you to verify the test results
     */
    FixtureExecutionResult whenTimeElapses(Duration elapsedTime);

    /**
     * Mimic an elapsed time with no relevant activity for the Saga. If any Events are scheduled to be published within
     * this time frame, they are published. All activity by the Saga on the CommandBus and EventBus (meaning that
     * scheduled events are excluded) is recorded.
     * <p/>
     * Note that if you inject resources using {@link FixtureConfiguration#registerResource(Object)}, you may need to
     * reset them yourself if they are manipulated by the Saga in the "given" stage of the test.
     *
     * @param newDateTime The time to advance the clock to
     * @return an object allowing you to verify the test results
     */
    FixtureExecutionResult whenTimeAdvancesTo(Instant newDateTime);
}
