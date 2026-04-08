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

import java.util.Map;


import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.core.Message;
import org.jspecify.annotations.NonNull;

public class ThrowingCorrelationDataProvider implements CorrelationDataProvider {

    @NonNull
    @Override
    public Map<String, String> correlationDataFor(@NonNull Message message) {
        throw new AxonConfigurationException("correlation is not clear");
    }
}
