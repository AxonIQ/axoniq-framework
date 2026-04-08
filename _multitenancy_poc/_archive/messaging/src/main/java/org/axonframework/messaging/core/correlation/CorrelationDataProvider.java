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

package org.axonframework.messaging.core.correlation;

import org.axonframework.messaging.core.Message;

import java.util.Map;

/**
 * Object defining the data from a {@link Message} that should be attached as correlation data to {@code Messages}
 * generated as result of the processing of the given {@code message}.
 *
 * @author Allard Buijze
 * @since 2.3.0
 */
@FunctionalInterface
public interface CorrelationDataProvider {

    /**
     * Provides a map with the entries to attach as correlation data to generated messages while processing given
     * {@code message}.
     * <p/>
     * This method should not return {@code null}. Any exception thrown from this method might interfere with rolling
     * back a transaction. Therefore, by default exceptions are caught, ignoring the correlation data that should have
     * been added.
     *
     * @param message The message to define correlation data for.
     * @return The data to attach as correlation data to generated messages.
     */
    Map<String, String> correlationDataFor(Message message);
}
