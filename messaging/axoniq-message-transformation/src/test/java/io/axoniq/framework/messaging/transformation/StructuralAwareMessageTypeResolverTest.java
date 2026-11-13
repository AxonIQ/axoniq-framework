/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation;

import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies {@link StructuralAwareMessageTypeResolver} resolves structural carrier types to
 * {@link Optional#empty()} without consulting the delegate, and delegates every other type to the
 * wrapped resolver.
 */
class StructuralAwareMessageTypeResolverTest {

    private static final MessageType DELEGATE_TYPE = new MessageType("com.example.Domain", "1.0.0");

    @Test
    void structuralTypesResolveToEmptyWithoutConsultingTheDelegate() {
        // given
        RecordingResolver delegate = new RecordingResolver();
        StructuralAwareMessageTypeResolver resolver = new StructuralAwareMessageTypeResolver(delegate);

        // when / then
        assertThat(resolver.resolve(Map.class)).isEmpty();
        assertThat(resolver.resolve(String.class)).isEmpty();
        assertThat(resolver.resolve(byte[].class)).isEmpty();
        assertThat(delegate.consulted).isFalse();
    }

    @Test
    void nonStructuralTypesDelegateToTheWrappedResolver() {
        // given
        RecordingResolver delegate = new RecordingResolver();
        StructuralAwareMessageTypeResolver resolver = new StructuralAwareMessageTypeResolver(delegate);

        // when
        Optional<MessageType> resolved = resolver.resolve(DomainPojo.class);

        // then
        assertThat(resolved).contains(DELEGATE_TYPE);
        assertThat(delegate.consulted).isTrue();
    }

    @Test
    void nullDelegateIsRejected() {
        assertThatThrownBy(() -> new StructuralAwareMessageTypeResolver(null))
                .isInstanceOf(NullPointerException.class);
    }

    /** Records whether the delegate was consulted, so the structural short-circuit can be asserted. */
    private static final class RecordingResolver implements MessageTypeResolver {

        private boolean consulted = false;

        @Override
        @NonNull
        public Optional<MessageType> resolve(@NonNull Class<?> payloadType) {
            consulted = true;
            return Optional.of(DELEGATE_TYPE);
        }
    }

    private static final class DomainPojo {
    }
}
