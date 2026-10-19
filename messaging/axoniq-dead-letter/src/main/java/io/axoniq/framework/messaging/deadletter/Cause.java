/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.deadletter;

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
