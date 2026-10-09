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

package org.axonframework.test.deadline;

import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.messaging.Scope;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Functional interface describing a {@link java.util.function.Consumer} of a {@link DeadlineMessage}.
 *
 * @author Milan Savic
 * @author Steven van Beelen
 * @since 3.3
 */
@FunctionalInterface
public interface DeadlineConsumer {

    /**
     * Consumes given {@code deadlineMessage}. The {@code deadlineScope} is used to identify the exact handler of the
     * message.
     *
     * @param deadlineScope   a description of the {@link Scope} in which a deadline was
     *                        scheduled
     * @param deadlineMessage the {@link DeadlineMessage} to be handled
     * @throws Exception in case something goes wrong while consuming the {@code deadlineMessage}
     */
    void consume(ScopeDescriptor deadlineScope, DeadlineMessage deadlineMessage) throws Exception;

    /**
     * Consumes given {@code deadlineMessage} within the given {@code context}, the {@link ProcessingContext} of the
     * unit of work the {@link StubDeadlineManager} fires the deadline in.
     * <p>
     * This is the method the {@code StubDeadlineManager} invokes. It defaults to
     * {@link #consume(ScopeDescriptor, DeadlineMessage)}, so a consumer written for Axon Framework 4 keeps working. A
     * consumer that hands the deadline to a {@link org.axonframework.messaging.ScopeAware} component overrides it, as
     * {@link org.axonframework.messaging.ScopeAware#send(org.axonframework.messaging.core.Message, ProcessingContext,
     * ScopeDescriptor) ScopeAware#send} needs that context. Axon Framework 4 found the unit of work in a thread-local
     * instead, which Axon Framework 5 no longer has.
     *
     * @param deadlineScope   a description of the {@link Scope} in which a deadline was scheduled
     * @param deadlineMessage the {@link DeadlineMessage} to be handled
     * @param context         the context of the unit of work the deadline is fired in
     * @throws Exception in case something goes wrong while consuming the {@code deadlineMessage}
     */
    default void consume(ScopeDescriptor deadlineScope,
                         DeadlineMessage deadlineMessage,
                         ProcessingContext context) throws Exception {
        consume(deadlineScope, deadlineMessage);
    }
}
