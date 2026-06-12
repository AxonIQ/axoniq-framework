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

package io.axoniq.framework.messaging.transformation.events;

import com.fasterxml.jackson.databind.JsonNode;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/**
 * The chain exposes its populated structure (transformation count, concrete-from buckets,
 * predicate-from list, max iterations) to framework diagnostics like
 * {@code AxonConfiguration.describe(...)} or Spring Boot Actuator endpoints. Operators rely
 * on these property names; the test pins them down.
 */
final class ChainDescribableComponentTest {

    private static final MessageType COURSE_V1 = new MessageType("com.example.CourseCreated", "1.0.0");
    private static final MessageType COURSE_V2 = new MessageType("com.example.CourseCreated", "2.0.0");
    private static final MessageType STUDENT_V1 = new MessageType("com.example.StudentRegistered", "1.0.0");
    private static final MessageType STUDENT_V2 = new MessageType("com.example.StudentRegistered", "2.0.0");

    @Test
    void describeToExposesTransformerCountConcreteAndPredicateTransformersAndMaxIterations() {
        EventTransformer concreteCourse = EventTransformer.from(COURSE_V1).to(COURSE_V2)
                                                              .transform(JsonNode.class, (in, ctx) -> in);
        EventTransformer concreteStudent = EventTransformer.from(STUDENT_V1).to(STUDENT_V2)
                                                               .transform(JsonNode.class, (in, ctx) -> in);
        EventTransformer predicateBased = EventTransformer.from(mt -> mt.version().startsWith("1."))
                                                              .to(new MessageType("com.example.AnyV2", "2.0.0"))
                                                              .transform(JsonNode.class, (in, ctx) -> in);
        EventTransformerChain chain = EventTransformerChain.builder()
                                                           .maxIterationsPerEvent(42)
                                                           .register(concreteCourse)
                                                           .register(concreteStudent)
                                                           .register(predicateBased)
                                                           .build();

        ComponentDescriptor descriptor = Mockito.mock(ComponentDescriptor.class);
        chain.describeTo(descriptor);

        verify(descriptor).describeProperty("transformerCount", 3);
        verify(descriptor).describeProperty("maxIterationsPerEvent", 42);

        ArgumentCaptor<Map<?, ?>> concreteCaptor = ArgumentCaptor.captor();
        verify(descriptor).describeProperty(eq("concreteTransformers"), concreteCaptor.capture());
        assertThat(concreteCaptor.getValue())
                .asInstanceOf(MAP)
                .containsKeys("com.example.CourseCreated", "com.example.StudentRegistered");
        // Bucket entries must be the transformer's toString(), not empty strings or null:
        // operators rely on these descriptions to know which transformer is in which bucket.
        assertThat(concreteCaptor.getValue().get("com.example.CourseCreated"))
                .asInstanceOf(LIST)
                .singleElement(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                .contains(concreteCourse.toString());
        assertThat(concreteCaptor.getValue().get("com.example.StudentRegistered"))
                .asInstanceOf(LIST)
                .singleElement(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                .contains(concreteStudent.toString());

        ArgumentCaptor<Collection<?>> predicateCaptor = ArgumentCaptor.captor();
        verify(descriptor).describeProperty(eq("predicateTransformers"), predicateCaptor.capture());
        assertThat(predicateCaptor.getValue())
                .asInstanceOf(LIST)
                .singleElement(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                .contains(predicateBased.toString());
    }

    @Test
    void describeToOnAnEmptyChainExposesAZeroCountAndEmptyContainers() {
        EventTransformerChain chain = EventTransformerChain.builder().build();

        ComponentDescriptor descriptor = Mockito.mock(ComponentDescriptor.class);
        chain.describeTo(descriptor);

        verify(descriptor).describeProperty("transformerCount", 0);

        ArgumentCaptor<Map<?, ?>> concreteCaptor = ArgumentCaptor.captor();
        verify(descriptor).describeProperty(eq("concreteTransformers"), concreteCaptor.capture());
        assertThat(concreteCaptor.getValue()).asInstanceOf(MAP).isEmpty();

        ArgumentCaptor<Collection<?>> predicateCaptor = ArgumentCaptor.captor();
        verify(descriptor).describeProperty(eq("predicateTransformers"), predicateCaptor.capture());
        assertThat(predicateCaptor.getValue()).asInstanceOf(LIST).isEqualTo(List.of());
    }
}
