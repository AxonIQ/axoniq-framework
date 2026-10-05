/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package org.axonframework.deadline;

import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * An {@link EventMessage} for a deadline, specified by its {@code deadlineName} and optionally containing a
 * {@code deadlinePayload}.
 * <p>
 * Implementations of the {@link DeadlineMessage} represent a fact (it's a specialization of {@code EventMessage}) that
 * some deadline was reached. The optional payload contains relevant data of the scheduled deadline.
 *
 * @author Milan Savic
 * @author Steven van Beelen
 * @since 3.3.0
 */
public interface DeadlineMessage extends EventMessage {

    /**
     * Returns the name of the {@link DeadlineMessage deadline} to be handled.
     *
     * @return the name of the {@link DeadlineMessage deadline}
     */
    String getDeadlineName();

    @Override
    DeadlineMessage withMetadata(Map<String, String> metadata);

    @Override
    DeadlineMessage andMetadata(Map<String, String> additionalMetadata);

    @Override
    default DeadlineMessage withConvertedPayload(Class<?> type, Converter converter) {
        return withConvertedPayload((Type) type, converter);
    }

    @Override
    default DeadlineMessage withConvertedPayload(TypeReference<?> type, Converter converter) {
        return withConvertedPayload(type.getType(), converter);
    }

    @Override
    DeadlineMessage withConvertedPayload(Type type, Converter converter);
}
