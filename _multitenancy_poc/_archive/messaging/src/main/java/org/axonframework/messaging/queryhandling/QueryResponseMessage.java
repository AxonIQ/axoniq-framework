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

package org.axonframework.messaging.queryhandling;

import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.ResultMessage;
import org.axonframework.conversion.Converter;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * A {@link ResultMessage} implementation that contains the result of query handling.
 *
 * @author Allard Buijze
 * @since 3.2.0
 */
public interface QueryResponseMessage extends ResultMessage {

    @Override
    QueryResponseMessage withMetadata(Map<String, String> metadata);

    @Override
    QueryResponseMessage andMetadata(Map<String, String> additionalMetadata);

    @Override
    default QueryResponseMessage withConvertedPayload(Class<?> type, Converter converter) {
        return withConvertedPayload((Type) type, converter);
    }

    @Override
    default QueryResponseMessage withConvertedPayload(TypeReference<?> type,
                                                      Converter converter) {
        return withConvertedPayload(type.getType(), converter);
    }

    @Override
    QueryResponseMessage withConvertedPayload(Type type, Converter converter);
}
