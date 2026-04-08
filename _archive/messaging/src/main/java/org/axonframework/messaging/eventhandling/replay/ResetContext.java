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

package org.axonframework.messaging.eventhandling.replay;

import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.Message;
import org.axonframework.conversion.Converter;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * A {@link Message} initiating the reset of an Event Handling Component.
 * <p>
 * A payload can be provided to support the reset operation handling this message.
 *
 * @author Steven van Beelen
 * @since 4.4.0
 */
public interface ResetContext extends Message {

    @Override
    ResetContext withMetadata(Map<String, String> metadata);

    @Override
    ResetContext andMetadata(Map<String, String> metadata);

    @Override
        default ResetContext withConvertedPayload(Class<?> type, Converter converter) {
        return withConvertedPayload((Type) type, converter);
    }

    @Override
        default ResetContext withConvertedPayload(TypeReference<?> type, Converter converter) {
        return withConvertedPayload(type.getType(), converter);
    }

    @Override
    ResetContext withConvertedPayload(Type type, Converter converter);
}
