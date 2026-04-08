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

package org.axonframework.messaging.core.annotation.namespace;

import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.AnnotationMessageTypeResolver;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that the {@link AnnotationMessageTypeResolver} resolves the
 * {@link org.axonframework.messaging.core.annotation.Namespace} annotation when present on a {@code package-info.java}
 * file.
 *
 * @author Steven van Beelen
 */
class NamespaceAnnotationMessageTypeResolverTest {

    private AnnotationMessageTypeResolver testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new AnnotationMessageTypeResolver();
    }

    @Test
    void namespaceAnnotationIsHonoredNamespaceAttribute() {
        Optional<MessageType> result = testSubject.resolve(MessageWithoutTypeNamespace.class);

        assertThat(result).isPresent();
        assertThat(result.get().qualifiedName().namespace()).isEqualTo("namespace-info");
    }

    @Event(name = "event")
    private record MessageWithoutTypeNamespace(String id) {

    }
}