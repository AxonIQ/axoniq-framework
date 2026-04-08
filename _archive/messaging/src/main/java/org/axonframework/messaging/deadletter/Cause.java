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

package org.axonframework.messaging.deadletter;

import org.axonframework.messaging.core.Message;

/**
 * Contract describing the cause for {@link DeadLetter dead lettering} a {@link Message}.
 * These objects typically reflects a {@link Throwable}.
 *
 * @author Steven van Beelen
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public interface Cause {

    /**
     * Returns the type of dead lettering cause. The {@code type} can, for example, reflect the fully qualified class
     * name of a {@link Throwable}.
     *
     * @return The type of this dead lettering cause.
     */
    String type();

    /**
     * A message describing a cause for dead lettering. The {@code message()} can, for example, reflect the message of a
     * {@link Throwable}.
     *
     * @return The message describing this cause's reason for dead lettering.
     */
    String message();
}
