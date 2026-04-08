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

package org.axonframework.test.util;

import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.Metadata;

/**
 * Default implementation of the CallbackBehavior interface. This implementation always returns {@code null}, which
 * results in the {@link org.axonframework.commandhandling.CommandCallback#onResult(CommandMessage,
 * CommandResultMessage)} method to be invoked with a {@code null} result
 * parameter.
 *
 * @author Allard Buijze
 * @since 2.0
 */
public class DefaultCallbackBehavior implements CallbackBehavior {

    @Override
    public Object handle(Object commandPayload, Metadata commandMetadata) {
        return null;
    }
}
