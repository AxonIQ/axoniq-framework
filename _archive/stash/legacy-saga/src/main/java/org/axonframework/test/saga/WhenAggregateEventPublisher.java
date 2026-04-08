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

import java.util.Map;

/**
 * Interface to an object that publishes events on behalf of an aggregate. The sequence number on the events must be
 * exactly sequential per aggregate.
 *
 * @author Allard Buijze
 * @since 1.1
 */
public interface WhenAggregateEventPublisher {

    /**
     * Register the given {@code event} to be published on behalf of an aggregate. Activity caused by this event
     * on the CommandBus and EventBus is monitored and can be checked in the FixtureExecutionResult.
     *
     * @param event The event published by the aggregate
     * @return a reference to the test results for the validation  phase
     */
    FixtureExecutionResult publishes(Object event);

    /**
     * Register the given {@code event} to be published on behalf of an aggregate, with given additional {@code metadata}.
     * Activity caused by this event on the CommandBus and EventBus is monitored and can be checked
     * in the FixtureExecutionResult.
     *
     * @param event The event published by the aggregate
     * @param metadata The metadata to attach to the event
     * @return a reference to the test results for the validation  phase
     */
    FixtureExecutionResult publishes(Object event, Map<String, String> metadata);
}
