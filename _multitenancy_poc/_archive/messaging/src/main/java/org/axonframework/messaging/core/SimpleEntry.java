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

package org.axonframework.messaging.core;

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.core.MessageStream.Entry;

import java.util.Map;
import java.util.function.Function;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * Simple implementation of the {@link Entry} containing a single {@link Message} implementation of type {@code M} and a
 * {@link Context}.
 *
 * @param <M>     The type of {@link Message} contained in this {@link Entry} implementation.
 * @param message The {@link Message} of type {@code M} contained in this {@link Entry}.
 * @param context Maintains additional contextual information of this entry.
 * @author Steven van Beelen
 * @since 5.0.0
 */
public record SimpleEntry<M extends Message>(@Nullable M message, Context context) implements Entry<M> {

    /**
     * Construct a SimpleEntry with the given {@code message} and an empty {@link Context}.
     *
     * @param message The {@link Message} of type {@code M} contained in this {@link Entry}.
     */
    public SimpleEntry(@Nullable M message) {
        this(message, Context.empty());
    }

    /**
     * Compact construct asserting the {@code context} is not {@code null}.
     *
     * @param context The context for this entry
     * @param message The message for this entry
     */
    public SimpleEntry {
        assertNonNull(context, "The context cannot be null");
    }

    @Override
    public <RM extends Message> Entry<RM> map(Function<M, RM> mapper) {
        return new SimpleEntry<>(
                message == null ? null : mapper.apply(message),
                context
        );
    }

    @Override
    public boolean containsResource(ResourceKey<?> key) {
        return this.context.containsResource(key);
    }

    @Override
    public <T> T getResource(ResourceKey<T> key) {
        return this.context.getResource(key);
    }

    @Override
    public <T> Entry<M> withResource(ResourceKey<T> key, T resource) {
        return new SimpleEntry<>(message, context.withResource(key, resource));
    }

    @Override
    public Map<ResourceKey<?>, Object> resources() {
        return this.context.resources();
    }
}
