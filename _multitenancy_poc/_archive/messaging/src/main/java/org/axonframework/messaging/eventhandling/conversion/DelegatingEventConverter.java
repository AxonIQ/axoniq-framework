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

package org.axonframework.messaging.eventhandling.conversion;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.lang.reflect.Type;
import java.util.Objects;

/**
 * An {@link EventConverter} implementation delegating conversion operations to a {@link MessageConverter}.
 * <p>
 * Useful to ensure callers of this component <b>only</b> convert {@link EventMessage} implementations.
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
public class DelegatingEventConverter implements EventConverter {

    private final MessageConverter delegate;

    /**
     * Constructs a {@code DelegatingEventConverter}, delegating operations to a {@link DelegatingMessageConverter}
     * build with the given {@code converter}.
     *
     * @param converter The converter to construct a {@link DelegatingMessageConverter} with to delegate all conversion
     *                  operations to.
     */
    public DelegatingEventConverter(Converter converter) {
        this(converter instanceof MessageConverter messageConverter
                     ? messageConverter
                     : new DelegatingMessageConverter(converter));
    }

    /**
     * Constructs a {@code DelegatingEventConverter}, delegating operations to the given {@code converter}.
     *
     * @param delegate The converter to delegate all conversion operations to.
     */
    public DelegatingEventConverter(MessageConverter delegate) {
        this.delegate = Objects.requireNonNull(delegate, "The Converter must not be null.");
    }

    @Nullable
    @Override
    public <T> T convert(@Nullable Object input, Type targetType) {
        return delegate.convert(input, targetType);
    }

    @Override
    @Nullable
    public <E extends EventMessage, T> T convertPayload(E event, Type targetType) {
        return delegate.convertPayload(event, targetType);
    }

    @Override
    public <E extends EventMessage> E convertEvent(E event, Type targetType) {
        return delegate.convertMessage(event, targetType);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("messageConverter", delegate);
    }

    /**
     * Returns the {@link MessageConverter} this {@code EventConverter} delegates to.
     * <p>
     * Useful to construct other instances with the exact same {@code Converter}.
     *
     * @return The {@link MessageConverter} this {@code EventConverter} delegates to.
     */
    @Internal
    public MessageConverter delegate() {
        return delegate;
    }
}
