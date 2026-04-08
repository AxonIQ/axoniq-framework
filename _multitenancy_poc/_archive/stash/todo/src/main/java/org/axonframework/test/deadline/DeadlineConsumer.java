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

package org.axonframework.test.deadline;

import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.messaging.core.Scope;
import org.axonframework.messaging.core.ScopeDescriptor;

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
     * @param deadlineScope   A description of the {@link Scope} in which a deadline was
     *                        scheduled
     * @param deadlineMessage the {@link DeadlineMessage} to be handled
     * @throws Exception in case something goes wrong while consuming the {@code deadlineMessage}
     */
    void consume(ScopeDescriptor deadlineScope, DeadlineMessage deadlineMessage) throws Exception;
}
