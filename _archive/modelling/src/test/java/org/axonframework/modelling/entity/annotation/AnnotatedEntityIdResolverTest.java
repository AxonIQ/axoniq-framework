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

package org.axonframework.modelling.entity.annotation;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.modelling.EntityIdResolutionException;
import org.axonframework.modelling.annotation.AnnotationBasedEntityIdResolver;
import org.axonframework.modelling.annotation.TargetEntityId;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.*;
import org.mockito.junit.jupiter.*;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class AnnotatedEntityIdResolverTest {

    @Mock
    private AnnotatedEntityMetamodel<String> metamodel;

    private AnnotatedEntityIdResolver<String> resolver;

    @BeforeEach
    void setUp() {
        resolver = new AnnotatedEntityIdResolver<>(metamodel,
                                                   String.class,
                                                   new DelegatingMessageConverter(new JacksonConverter()),
                                                   new AnnotationBasedEntityIdResolver<>());
    }

    @Test
    void canResolveIdFromSerializedMessage() throws EntityIdResolutionException {
        MessageType messageType = new MessageType(MyIdHoldingObject.class);
        var serializedMessage = new GenericMessage(messageType, """
                {"identifier": "test5362"}""");

        QualifiedName qualifiedName = messageType.qualifiedName();
        Mockito.doReturn(MyIdHoldingObject.class)
               .when(metamodel)
               .getExpectedRepresentation(qualifiedName);


        String resolvedId = resolver.resolve(serializedMessage, new StubProcessingContext());
        assertEquals("test5362", resolvedId);
    }

    @Test
    void shouldThrowExceptionWhenUnableToResolveId() {
        MessageType messageType = new MessageType(MyIdHoldingObject.class);
        var serializedMessage = new GenericMessage(messageType, "{}");

        QualifiedName qualifiedName = messageType.qualifiedName();
        Mockito.doReturn(MyIdHoldingObject.class)
               .when(metamodel)
               .getExpectedRepresentation(qualifiedName);

        assertThatThrownBy(() -> resolver.resolve(serializedMessage, new StubProcessingContext()))
                .isInstanceOf(EntityIdResolutionException.class);
    }

    @Test
    void throwsExceptionWhenExpectedRepresentationIsMissing() {
        MessageType messageType = new MessageType(MyIdHoldingObject.class);
        var serializedMessage = new GenericMessage(messageType, """
                {"identifier": "test5362"}""");

        assertThrows(AxonConfigurationException.class, () -> {
            resolver.resolve(serializedMessage, new StubProcessingContext());
        });
    }


    private record MyIdHoldingObject(
            @TargetEntityId String identifier
    ) {

    }
}