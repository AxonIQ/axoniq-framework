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

package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.association.Associations;
import io.axoniq.framework.workflow.dsl.api.EventCondition;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.runtime.api.execution.context.Version;
import io.axoniq.framework.workflow.runtime.api.payload.PayloadReducer;
import io.axoniq.framework.workflow.runtime.execution.payload.PayloadReducerRegistry;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer.NAME;
import static io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test for {@link EventWaitConditions}.
 */
class EventWaitConditionsTest {

    private EventWaitConditions eventWaitConditions;
    private final PayloadReducerRegistry registry = new PayloadReducerRegistry();
    private final PayloadReducer globalOnly = registry.get(NAME).orElseThrow();

    @BeforeEach
    void setUp() {
        eventWaitConditions = new EventWaitConditions();
    }

    @Test
    void addAndRemove() {
        QualifiedName qName = new QualifiedName("ns", "Event");
        EventCondition condition = EventConditions.fromQualifiedName(qName);
        eventWaitConditions.add("step1", condition, globalOnly, defaults());

        ComponentDescriptor descriptor = mock(ComponentDescriptor.class);
        eventWaitConditions.describeTo(descriptor);
        verify(descriptor).describeProperty(eq("waitConditions"), any(Collection.class));

        eventWaitConditions.remove("step1");

        assertThat(getDescribedConditions()).isEmpty();
    }

    @Test
    void evaluateAndApplyMatches() {
        QualifiedName qName = new QualifiedName("ns", "Event");
        EventCondition condition = EventConditions.fromQualifiedName(qName);
        eventWaitConditions.add("step1", condition, globalOnly, defaults());

        EventMessage message = mock(EventMessage.class);
        ProcessingContext pc = mock(ProcessingContext.class);
        when(message.type()).thenReturn(new MessageType(qName, Version.DEFAULT_VERSION));

        AtomicReference<EventMessage> appliedMessage = new AtomicReference<>();
        eventWaitConditions.evaluateAndApply(message, pc, awaited -> appliedMessage.set(awaited.eventMessage()));

        assertThat(appliedMessage.get()).isEqualTo(message);

        // Should be removed after apply
        assertThat(getDescribedConditions()).isEmpty();
    }

    @Test
    void evaluateAndApplyNoMatch() {
        QualifiedName qName = new QualifiedName("ns", "Event");
        EventCondition condition = EventConditions.never(qName);
        eventWaitConditions.add("step1", condition, globalOnly, defaults());

        EventMessage message = mock(EventMessage.class);
        ProcessingContext pc = mock(ProcessingContext.class);

        when(message.type()).thenReturn(new MessageType(qName, Version.DEFAULT_VERSION));

        AtomicReference<EventMessage> appliedMessage = new AtomicReference<>();
        eventWaitConditions.evaluateAndApply(message, pc, awaited -> appliedMessage.set(awaited.eventMessage()));

        assertThat(appliedMessage.get()).isNull();

        // Should NOT be removed
        assertThat(getDescribedConditions()).hasSize(1);
    }

    @Test
    void describeTo() {
        QualifiedName qName1 = new QualifiedName("ns", "Event1");
        EventCondition condition = EventConditions.fromQualifiedName(qName1);
        eventWaitConditions.add("step1", condition, globalOnly, defaults());

        QualifiedName qName2 = new QualifiedName("ns", "Event2");
        EventCondition condition2 = EventConditions.fromQualifiedName(qName2);
        eventWaitConditions.add("step2", condition2, globalOnly, defaults());


        Collection<?> list = getDescribedConditions();
        assertThat(list).hasSize(2);

        for (Object obj : list) {
            assertThat(obj).isInstanceOf(DescribableComponent.class);
            DescribableComponent desc = (DescribableComponent) obj;
            ComponentDescriptor subDescriptor = mock(ComponentDescriptor.class);
            desc.describeTo(subDescriptor);

            ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
            verify(subDescriptor).describeProperty(nameCaptor.capture(), valueCaptor.capture());

            String stepName = nameCaptor.getValue();
            String qNameStr = valueCaptor.getValue();

            if ("step1".equals(stepName)) {
                assertThat(qNameStr).isEqualTo(qName1.toString());
            } else if ("step2".equals(stepName)) {
                assertThat(qNameStr).isEqualTo(qName2.toString());
            } else {
                assertThat(stepName).isIn("step1", "step2");
            }
        }
    }

    @Test
    void describeToIncludesSerializedAssociations() {
        QualifiedName qName = new QualifiedName("ns", "Event");
        EventCondition condition = EventConditions.fromQualifiedName(
                qName,
                Associations.associate(payloadProperty("orderId"), "=", "123")
        );
        eventWaitConditions.add("step1", condition, globalOnly, defaults());

        DescribableComponent desc = (DescribableComponent) getDescribedConditions().iterator().next();
        ComponentDescriptor subDescriptor = mock(ComponentDescriptor.class);
        desc.describeTo(subDescriptor);

        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(subDescriptor).describeProperty(eq("step1"), valueCaptor.capture());
        assertThat(valueCaptor.getValue()).isEqualTo("ns.Event where payload:orderId=123");
    }

    private Collection<?> getDescribedConditions() {
        ComponentDescriptor descriptor = mock(ComponentDescriptor.class);
        eventWaitConditions.describeTo(descriptor);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<?>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(descriptor).describeProperty(eq("waitConditions"), captor.capture());
        return captor.getValue();
    }
}
