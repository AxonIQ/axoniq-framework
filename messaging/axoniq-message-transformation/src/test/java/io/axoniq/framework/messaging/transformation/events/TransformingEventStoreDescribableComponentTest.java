/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation.events;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.verify;

/**
 * The decorator exposes its delegate, chain, converter, and resolver via
 * {@link TransformingEventStore#describeTo} for framework diagnostics
 * ({@code AxonConfiguration.describe(...)} / Spring Boot Actuator endpoints). Operators
 * rely on these property names; the test pins them down.
 */
final class TransformingEventStoreDescribableComponentTest {

    @Test
    void describeToExposesDelegateChainConverterAndMessageTypeResolver() {
        EventStore delegate = Mockito.mock(EventStore.class);
        EventTransformerChain chain = EventTransformerChain.builder().build();
        MessageConverter converter = Mockito.mock(MessageConverter.class);
        MessageTypeResolver resolver = Mockito.mock(MessageTypeResolver.class);
        TransformingEventStore decorated = new TransformingEventStore(delegate, chain, converter, resolver);

        ComponentDescriptor descriptor = Mockito.mock(ComponentDescriptor.class);
        decorated.describeTo(descriptor);

        verify(descriptor).describeProperty("delegate", delegate);
        verify(descriptor).describeProperty("chain", chain);
        verify(descriptor).describeProperty("converter", converter);
        verify(descriptor).describeProperty("messageTypeResolver", resolver);
    }
}
