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

package org.axonframework.messaging.eventhandling;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;

/**
 * Empty {@link EventMessage} implementation without any {@link EventMessage#payload() payload}, used as the
 * <b>terminal</b> message of a {@link MessageStream}. This thus signals the end of the
 * {@code MessageStream}.
 * <p>
 * Only useful to be paired with {@link Context} information in an event-specific
 * {@code MessageStream} when there is no event payload to combine it with.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
@Internal
public class TerminalEventMessage extends GenericEventMessage {

    /**
     * The sole instance of the {@link TerminalEventMessage}.
     */
    public static final TerminalEventMessage INSTANCE = new TerminalEventMessage();

    private TerminalEventMessage() {
        super(new MessageType(TerminalEventMessage.class), null);
    }
}
