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

package io.axoniq.workflow.configuration;

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.workflow.runtime.association.ValueComparisonOperatorRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.annotation.AnnotationMessageTypeResolver;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static io.axoniq.workflow.runtime.api.annotation.Workflow.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class AutoDetectionUtilsTest {

    @Test
    void shouldValidateAttributesSuccessfully() throws NoSuchMethodException {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ATTR_ID_PROPERTY, "myId");
        attributes.put(ATTR_START_ON_EVENT_CLASS, Object.class);

        Method method = TestWorkflow.class.getMethod("myWorkflow");
        Class<?> type = TestWorkflow.class;

        assertThatCode(() -> AutoDetectionUtils.validateAttributes(attributes, type, method))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldValidateAttributesSuccessfullyWithFqn() throws NoSuchMethodException {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ATTR_ID_PROPERTY, "myId");
        attributes.put(ATTR_START_ON_EVENT_NAME, "io.axoniq.MyEvent");

        Method method = TestWorkflow.class.getMethod("myWorkflow");
        Class<?> type = TestWorkflow.class;

        assertThatCode(() -> AutoDetectionUtils.validateAttributes(attributes, type, method))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldValidateAttributesSuccessfullyWithIdProvider() throws NoSuchMethodException {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ATTR_ID_PROPERTY_PROVIDER, MyIdProvider.class);
        attributes.put(ATTR_START_ON_EVENT_CLASS, Object.class);

        Method method = TestWorkflow.class.getMethod("myWorkflow");
        Class<?> type = TestWorkflow.class;

        assertThatCode(() -> AutoDetectionUtils.validateAttributes(attributes, type, method))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldFailValidationIfIdMissing() throws NoSuchMethodException {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ATTR_START_ON_EVENT_CLASS, Object.class);
        // Default idPropertyProvider is PayloadPropertyWorkflowIdProvider
        attributes.put(ATTR_ID_PROPERTY_PROVIDER, WorkflowIdProvider.class);

        Method method = TestWorkflow.class.getMethod("myWorkflow");
        Class<?> type = TestWorkflow.class;

        assertThatThrownBy(() -> AutoDetectionUtils.validateAttributes(attributes, type, method))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Either idProperty or idPropertyProvider must be specified");
    }

    @Test
    void shouldFailValidationIfStartOnMissing() throws NoSuchMethodException {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ATTR_ID_PROPERTY, "myId");
        attributes.put(ATTR_START_ON_EVENT_CLASS, Void.class);
        attributes.put(ATTR_START_ON_EVENT_NAME, "");

        Method method = TestWorkflow.class.getMethod("myWorkflow");
        Class<?> type = TestWorkflow.class;

        assertThatThrownBy(() -> AutoDetectionUtils.validateAttributes(attributes, type, method))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Either " + ATTR_START_ON_EVENT_NAME + " or " + ATTR_START_ON_EVENT_CLASS
                                              + " must be specified");
    }

    @Test
    void shouldFailValidationIfBothStartOnAttributesAreSpecified() throws NoSuchMethodException {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ATTR_ID_PROPERTY, "myId");
        attributes.put(ATTR_START_ON_EVENT_CLASS, String.class);
        attributes.put(ATTR_START_ON_EVENT_NAME, "some.package.Type");

        Method method = TestWorkflow.class.getMethod("myWorkflow");
        Class<?> type = TestWorkflow.class;

        assertThatThrownBy(() -> AutoDetectionUtils.validateAttributes(attributes, type, method))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Either " + ATTR_START_ON_EVENT_NAME + " or " + ATTR_START_ON_EVENT_CLASS
                                              + ", but not both must be specified");
    }

    @Test
    void shouldPreferFqnOverClassInEventCondition() {
        Configuration configuration = mock(Configuration.class);
        MessageTypeResolver messageTypeResolver = mock(MessageTypeResolver.class);
        ValueComparisonOperatorRegistry operatorRegistry = new ValueComparisonOperatorRegistry();
        when(configuration.getComponent(eq(MessageTypeResolver.class))).thenReturn(messageTypeResolver);
        when(configuration.getComponent(eq(ValueComparisonOperatorRegistry.class), any(Supplier.class))).thenReturn(
                operatorRegistry);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ATTR_START_ON_EVENT_NAME, "io.axoniq.MyEvent");
        attributes.put(ATTR_START_ON_EVENT_CLASS, String.class);
        attributes.put(ATTR_START_ON_CONDITIONS, new String[0]);

        EventCondition condition = AutoDetectionUtils.eventConditionComponentBuilder(attributes).build(configuration);

        assertThat(condition.qualifiedName()).isEqualTo(new QualifiedName("io.axoniq.MyEvent"));
        verifyNoInteractions(messageTypeResolver);
    }

    @Test
    void shouldFallbackToClassInEventCondition() {
        Configuration configuration = mock(Configuration.class);
        MessageTypeResolver messageTypeResolver = new AnnotationMessageTypeResolver();
        ValueComparisonOperatorRegistry operatorRegistry = new ValueComparisonOperatorRegistry();
        when(configuration.getComponent(eq(MessageTypeResolver.class))).thenReturn(messageTypeResolver);
        when(configuration.getComponent(eq(ValueComparisonOperatorRegistry.class), any(Supplier.class))).thenReturn(
                operatorRegistry);

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(ATTR_START_ON_EVENT_NAME, "");
        attributes.put(ATTR_START_ON_EVENT_CLASS, AnnotatedEvent.class);
        attributes.put(ATTR_START_ON_CONDITIONS, new String[0]);

        EventCondition condition = AutoDetectionUtils.eventConditionComponentBuilder(attributes).build(configuration);

        assertThat(condition.qualifiedName()).isEqualTo(new QualifiedName("custom.AnnotatedEvent"));
    }

    @Event(namespace = "custom", name = "AnnotatedEvent")
    private record AnnotatedEvent(String id) {

    }

    private static class TestWorkflow {

        public void myWorkflow() {
        }
    }

    private static class MyIdProvider implements WorkflowIdProvider {

        @Override
        public String apply(org.axonframework.messaging.eventhandling.EventMessage eventMessage) {
            return "id";
        }
    }
}
