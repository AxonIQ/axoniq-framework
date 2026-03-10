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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.association;

import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.function.BiFunction;

/**
 * Retrieves association value from the event message.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@FunctionalInterface
public interface ValueRetriever extends BiFunction<EventMessage, Converter, Object> {

}
