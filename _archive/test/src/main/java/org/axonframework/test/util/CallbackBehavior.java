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

import org.axonframework.messaging.core.Metadata;

/**
 * Interface towards a mechanism that replicates the behavior of a Command Handling component. The goal of this
 * component is to mimic behavior on the callback.
 *
 * @author Allard Buijze
 * @since 2.0
 */
public interface CallbackBehavior {

    /**
     * Invoked when the Command Bus receives a Command that is dispatched with a Callback method. The return value of
     * this invocation is used to invoke the callback.
     *
     * @param commandPayload  The payload of the Command Message
     * @param commandMetadata The Metadata of the CommandMessage
     * @return any return value to pass to the callback's onResult method.
     *
     * @throws Exception If the onFailure method of the callback must be invoked
     */
    Object handle(Object commandPayload, Metadata commandMetadata) throws Exception;
}
