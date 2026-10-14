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

import org.axonframework.common.annotation.Internal;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.DelegatingGeneralConverter;
import org.axonframework.conversion.xstream.XStreamConverter;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;

import java.util.Optional;
import java.util.function.Function;

/**
 * Resolves the type names that Axon Framework 4's {@code XStreamSerializer} stored next to a deadline's payload and
 * scope descriptor through the mapper of the configured {@link XStreamConverter}, so that aliased types resolve.
 * <p>
 * Only {@link StoredDeadlineConverter} uses this class. It is kept apart so that XStream, an optional dependency, is
 * only loaded when the configured converter writes the XStream format.
 *
 * @author Jakob Hatzl
 * @since 5.4.0
 */
final class XStreamStoredTypeNames {

    private XStreamStoredTypeNames() {
        // Utility class
    }

    /**
     * Returns the lookup of stored type names for the given {@code converter}: through the mapper of its
     * {@link XStreamConverter}, or a lookup resolving nothing if it is another converter writing the XStream format.
     * <p>
     * The {@code XStreamConverter} is also found when it is wrapped in a {@link DelegatingGeneralConverter},
     * {@link DelegatingMessageConverter} or {@link DelegatingEventConverter}, as an application registering it as its
     * general or event converter passes it on.
     *
     * @param converter the converter the deadlines are stored with
     * @return the lookup of stored type names
     */
    @SuppressWarnings("removal")
    static Function<String, Optional<Class<?>>> lookupFor(Converter converter) {
        if (unwrap(converter) instanceof XStreamConverter xStreamConverter) {
            return xStreamConverter::resolveStoredType;
        }
        return typeName -> Optional.empty();
    }

    private static Converter unwrap(Converter converter) {
        return switch (converter) {
            case DelegatingEventConverter delegating -> unwrap(delegating.delegate());
            case DelegatingMessageConverter delegating -> unwrap(delegating.delegate());
            case DelegatingGeneralConverter delegating -> unwrap(delegating.delegate());
            default -> converter;
        };
    }
}
