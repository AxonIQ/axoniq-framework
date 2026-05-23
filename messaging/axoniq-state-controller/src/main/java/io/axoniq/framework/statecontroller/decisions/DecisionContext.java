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

package io.axoniq.framework.statecontroller.decisions;

import io.axoniq.framework.statecontroller.eventstream.EventStream;

import java.time.Instant;
import java.util.Map;

/**
 * The handle a {@link StateController @StateController} decision method receives to declare the slice of event
 * history it cares about and to read decision-time information.
 * <p>
 * {@link #scope(String, Object)} declares a single-tag scope (the common case);
 * {@link #scope(Map)} declares a composite scope across multiple tags. Each {@code scope(...)} call returns an
 * {@link EventStream} bound to a loading-context shared with the conditions it produces, so the framework can
 * load every declared question in a single read.
 * <p>
 * {@link #time()} returns the decision-time {@link Instant}, made explicit so decisions stay testable and
 * independent of wall-clock side effects.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public interface DecisionContext {

    /**
     * Returns an {@link EventStream} containing the events tagged with the given key/value pair.
     *
     * @param tagKey   the tag key identifying the entity or slice this decision concerns
     * @param tagValue the tag value, typically an entity identifier; the framework converts it to a string
     * @return the lazily-loaded event stream for the tagged slice
     */
    EventStream scope(String tagKey, Object tagValue);

    /**
     * Returns an {@link EventStream} containing the events tagged with all of the given key/value pairs.
     * <p>
     * Use this overload when a decision spans multiple identifiers (for example, the source and destination
     * accounts in a transfer). The framework will resolve the union of relevant events for the composite scope.
     *
     * @param tags the tag key/value pairs defining the scope; the framework converts values to strings
     * @return the lazily-loaded event stream for the composite tagged slice
     */
    EventStream scope(Map<String, ?> tags);

    /**
     * Returns the time at which this decision is being made.
     * <p>
     * Decision methods should prefer this method over {@link Instant#now()} so that decisions remain deterministic
     * under test and replay.
     *
     * @return the decision-time instant
     */
    Instant time();
}
