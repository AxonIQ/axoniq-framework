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

package org.axonframework.modelling.command;

import org.axonframework.messaging.core.Message;

import java.util.function.Supplier;

/**
 * Interface that describes a mechanism that enables the application of an additional event after another event is
 * applied.
 * <p>
 * The most common application of this mechanism is in the event handler method of an aggregate. Say that an event
 * handler for an event of type A applies another event of type B and then an event of type C. Only after the processing
 * of event A is finished will event B be applied to the aggregate. That means that the effects of event B will not
 * be visible from within the event handler. This is a problem if event C depends on the changes caused by
 * event B. The ApplyMore mechanism solves this problem by allowing clients to apply additional events after previous
 * events have been fully processed.
 * <p>
 * Be careful not to make any direct changes to the aggregate from the supplier of the subsequent event. If the
 * aggregate is event sourced this will cause the aggregate to be in an inconsistent state after event sourcing.
 */
public interface ApplyMore {

    /**
     * Apply a subsequent event to the aggregate after applying another event. When the given {@code
     * payloadOrMessageSupplier} is asked to provide the subsequent event the initial event has been fully processed by
     * the aggregate.
     * <p>
     * If the given supplier passes an object that is an instance of a {@link Message} the
     * event is applied with the metadata from the message. If the supplied event is not a Message instance it will be
     * applied as an event without additional metadata.
     *
     * @param payloadOrMessageSupplier The next event message or the payload of the next event
     * @return an instance of ApplyMore to apply any subsequent events
     */
    ApplyMore andThenApply(Supplier<?> payloadOrMessageSupplier);

    /**
     * Conditionally apply a subsequent event to the aggregate after applying another event. When the given {@code
     * payloadOrMessageSupplier} is asked to provide the subsequent event the initial event has been fully processed by
     * the aggregate.
     * <p>
     * If the given supplier passes an object that is an instance of a {@link Message} the
     * event is applied with the metadata from the message. If the supplied event is not a Message instance it will be
     * applied as an event without additional metadata.
     *
     * @param condition                the condition to evaluate and decide if the payload should be applied
     * @param payloadOrMessageSupplier The next event message or the payload of the next event
     * @return an instance of ApplyMore to apply any subsequent events
     */
    default ApplyMore andThenApplyIf(Supplier<Boolean> condition, Supplier<?> payloadOrMessageSupplier) {
        if (condition.get()) {
            return andThenApply(payloadOrMessageSupplier);
        } else {
            return this;
        }
    }

    /**
     * Execute the given {@code runnable} after applying the previous event. This {@code runnable} is guaranteed to be
     * invoked when the previous event has been fully applied to the aggregate.
     * <p>
     * The given {@code runnable} must not directly alter any state of the aggregate. Instead, it should only decide
     * if more events should be applied based on the state of the aggregate after the previous event
     *
     * @param runnable the code to execute when the previous event was applied
     * @return an instance of ApplyMore to apply any subsequent events
     */
    ApplyMore andThen(Runnable runnable);

    /**
     * Conditionally execute the given {@code runnable} after applying the previous event. This {@code runnable} is
     * guaranteed to be invoked when the previous event has been fully applied to the aggregate.
     * <p>
     * The given {@code runnable} must not directly alter any state of the aggregate. Instead, it should only decide
     * if more events should be applied based on the state of the aggregate after the previous event
     *
     * @param condition the condition to evaluate and decide if the code should be executed
     * @param runnable  the code to execute when the previous event was applied
     * @return an instance of ApplyMore to apply any subsequent events
     */
    default ApplyMore andThenIf(Supplier<Boolean> condition, Runnable runnable) {
        if (condition.get()) {
            return andThen(runnable);
        } else {
            return this;
        }
    }
}
