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
package io.axoniq.workflow.runtime.execution;

import org.jspecify.annotations.Nullable;

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry.PredicatedWorkflowConfiguration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link SimpleWorkflowConfigurationRegistry}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class SimpleWorkflowConfigurationRegistryTest {

    private SimpleWorkflowConfigurationRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SimpleWorkflowConfigurationRegistry();
    }

    @Test
    void testRegisterAndGetConfigurations() {
        QualifiedName eventName = new QualifiedName("com.example.MyEvent");
        BiPredicate<EventMessage, ProcessingContext> predicate = (eventMessage, pc) -> true;
        EventCondition eventCondition = eventCondition(eventName, predicate);

        WorkflowConfiguration<?> workflowConfiguration = new StubWorkflowConfiguration();

        registry.register(eventCondition, workflowConfiguration);

        Set<QualifiedName> supportedEvents = registry.supportedEvents();
        assertThat(supportedEvents).contains(eventName);
        assertThat(supportedEvents).hasSize(1);

        List<PredicatedWorkflowConfiguration> configs = registry.getWorkflowsConfigurations(eventName);
        assertThat(configs).hasSize(1);
        assertThat(configs.getFirst().predicate()).isEqualTo(predicate);
        assertThat(configs.getFirst().configuration()).isEqualTo(workflowConfiguration);
    }

    @Test
    void testRegisterMultipleConfigurationsForSameEvent() {
        QualifiedName eventName = new QualifiedName("com.example.MyEvent");

        BiPredicate<EventMessage, ProcessingContext> pred1 = (msg, pc) -> true;
        EventCondition cond1 = eventCondition(eventName, pred1);
        WorkflowConfiguration<?> conf1 = new StubWorkflowConfiguration();

        BiPredicate<EventMessage, ProcessingContext> pred2 = (msg, pc) -> false;
        EventCondition cond2 = eventCondition(eventName, pred2);
        WorkflowConfiguration<?> conf2 = new StubWorkflowConfiguration();

        registry.register(cond1, conf1);
        registry.register(cond2, conf2);

        List<PredicatedWorkflowConfiguration> configs = registry.getWorkflowsConfigurations(eventName);
        assertThat(configs).hasSize(2);

        assertThat(configs).anyMatch(c -> c.predicate().equals(pred1) && c.configuration().equals(conf1));
        assertThat(configs).anyMatch(c -> c.predicate().equals(pred2) && c.configuration().equals(conf2));
    }

    @Test
    void testSupportedEvents() {
        QualifiedName event1 = new QualifiedName("Event1");
        QualifiedName event2 = new QualifiedName("Event2");

        registry.register(event1, new StubWorkflowConfiguration());
        registry.register(event2, new StubWorkflowConfiguration());

        Set<QualifiedName> supportedEvents = registry.supportedEvents();
        assertThat(supportedEvents).hasSize(2);
        assertThat(supportedEvents).contains(event1);
        assertThat(supportedEvents).contains(event2);
    }

    @Test
    void testGetWorkflowsConfigurationsReturnsEmptyListForUnknownEvent() {
        List<PredicatedWorkflowConfiguration> configs = registry.getWorkflowsConfigurations(new QualifiedName("Unknown"));
        assertThat(configs).isNotNull();
        assertThat(configs).isEmpty();
    }

    @Test
    void getHighestVersionConfigurationsReturnsEmptyListForUnknownEvent() {
        assertThat(registry.getHighestVersionConfigurations(new QualifiedName("Unknown"))).isEmpty();
    }

    @Test
    void getHighestVersionConfigurationsReturnsOnlyTheHighestRegisteredVersion() {
        QualifiedName eventName = new QualifiedName("com.example.OrderPlaced");
        registry.register(eventName, new VersionedStub("OrderWorkflow", "1.0.0"));
        registry.register(eventName, new VersionedStub("OrderWorkflow", "2.0.0"));

        var configs = registry.getHighestVersionConfigurations(eventName);
        assertThat(configs).hasSize(1);
        assertThat(configs.getFirst().configuration().workflowVersion()).isEqualTo("2.0.0");
    }

    @Test
    void getHighestVersionConfigurationsReflectsLaterRegistrations() {
        QualifiedName eventName = new QualifiedName("com.example.OrderPlaced");
        registry.register(eventName, new VersionedStub("OrderWorkflow", "1.0.0"));
        assertThat(registry.getHighestVersionConfigurations(eventName))
                .singleElement()
                .satisfies(c -> assertThat(c.configuration().workflowVersion()).isEqualTo("1.0.0"));

        registry.register(eventName, new VersionedStub("OrderWorkflow", "2.0.0"));
        assertThat(registry.getHighestVersionConfigurations(eventName))
                .singleElement()
                .satisfies(c -> assertThat(c.configuration().workflowVersion()).isEqualTo("2.0.0"));
    }

    @Test
    void getHighestVersionConfigurationsReturnsAllConfigurationsAtTheHighestVersion() {
        QualifiedName eventName = new QualifiedName("com.example.OrderPlaced");
        registry.register(eventName, new VersionedStub("OrderWorkflow", "2.0.0"));
        registry.register(eventName, new VersionedStub("OtherWorkflow", "2.0.0"));
        registry.register(eventName, new VersionedStub("OrderWorkflow", "1.0.0"));

        var configs = registry.getHighestVersionConfigurations(eventName);
        assertThat(configs).hasSize(2);
        assertThat(configs).allSatisfy(c -> assertThat(c.configuration().workflowVersion()).isEqualTo("2.0.0"));
    }

    @Test
    void testDescribeTo() {
        QualifiedName eventName = new QualifiedName("com.example.MyEvent");
        WorkflowDefinition<WorkflowContext> definition = mock(WorkflowDefinition.class);
        WorkflowConfiguration<?> config = new StubWorkflowConfiguration(definition);

        registry.register(eventName, config);

        ComponentDescriptor descriptor = mock(ComponentDescriptor.class);
        registry.describeTo(descriptor);

        ArgumentCaptor<List> listCaptor = ArgumentCaptor.forClass(List.class);
        verify(descriptor).describeProperty(eq("workflowDefinitions"), listCaptor.capture());

        List<?> descriptors = listCaptor.getValue();
        assertThat(descriptors).hasSize(1);
        Object descObj = descriptors.get(0);

        // The descriptor is an internal record WorkflowDefinitionDescriptor, which is DescribableComponent
        assertThat(descObj).isInstanceOf(org.axonframework.common.infra.DescribableComponent.class);

        ComponentDescriptor subDescriptor = mock(ComponentDescriptor.class);
        ((org.axonframework.common.infra.DescribableComponent) descObj).describeTo(subDescriptor);

        verify(subDescriptor).describeProperty(eq(eventName.toString()), any(List.class));
    }

    @Test
    void findClosestRegisteredVersionReturnsExactMatchWhenPresent() {
        QualifiedName eventName = new QualifiedName("com.example.OrderPlaced");
        registry.register(eventName, new VersionedStub("OrderWorkflow", "1.0.0"));
        registry.register(eventName, new VersionedStub("OrderWorkflow", "2.0.0"));

        var match = registry.findClosestRegisteredVersion("OrderWorkflow", "1.0.0");
        assertThat(match).isPresent();
        assertThat(match.get().workflowVersion()).isEqualTo("1.0.0");
    }

    @Test
    void findClosestRegisteredVersionPicksHighestBelowOrEqualToRequested() {
        // v1 = 1.0.0, v2 = 2.0.0 registered. State recorded "1.0.1" (after a ctx.migrateVersion bump that
        // no exact sibling matches). Closest match should be v1.0.0, NOT v2.0.0 - semantically a v1
        // workflow that bumped to 1.0.1 is closer to v1.0.0 than to v2.0.0.
        QualifiedName eventName = new QualifiedName("com.example.OrderPlaced");
        registry.register(eventName, new VersionedStub("OrderWorkflow", "1.0.0"));
        registry.register(eventName, new VersionedStub("OrderWorkflow", "2.0.0"));

        var match = registry.findClosestRegisteredVersion("OrderWorkflow", "1.0.1");
        assertThat(match).isPresent();
        assertThat(match.get().workflowVersion())
                .as("Closest registered version <= 1.0.1 should be 1.0.0")
                .isEqualTo("1.0.0");
    }

    @Test
    void findClosestRegisteredVersionPicksHighestWhenRequestedIsAboveAllRegistered() {
        QualifiedName eventName = new QualifiedName("com.example.OrderPlaced");
        registry.register(eventName, new VersionedStub("OrderWorkflow", "1.0.0"));
        registry.register(eventName, new VersionedStub("OrderWorkflow", "2.0.0"));

        // State recorded "2.0.1" - still closest to v2.0.0 (highest <= 2.0.1).
        var match = registry.findClosestRegisteredVersion("OrderWorkflow", "2.0.1");
        assertThat(match).isPresent();
        assertThat(match.get().workflowVersion()).isEqualTo("2.0.0");
    }

    @Test
    void findClosestRegisteredVersionReturnsEmptyWhenAllRegisteredAreAboveRequested() {
        // State recorded "0.5.0" but only v1.0.0 / v2.0.0 are registered. No registered version is
        // <= 0.5.0 - return empty so the caller can fall back to its start-time configuration.
        QualifiedName eventName = new QualifiedName("com.example.OrderPlaced");
        registry.register(eventName, new VersionedStub("OrderWorkflow", "1.0.0"));
        registry.register(eventName, new VersionedStub("OrderWorkflow", "2.0.0"));

        var match = registry.findClosestRegisteredVersion("OrderWorkflow", "0.5.0");
        assertThat(match).isEmpty();
    }

    @Test
    void findClosestRegisteredVersionFiltersOutOtherWorkflowNames() {
        QualifiedName eventName = new QualifiedName("com.example.OrderPlaced");
        registry.register(eventName, new VersionedStub("OrderWorkflow", "1.0.0"));
        registry.register(eventName, new VersionedStub("PaymentWorkflow", "2.0.0"));

        var match = registry.findClosestRegisteredVersion("OrderWorkflow", "5.0.0");
        assertThat(match).isPresent();
        assertThat(match.get().workflowName()).isEqualTo("OrderWorkflow");
        assertThat(match.get().workflowVersion()).isEqualTo("1.0.0");
    }

    @Test
    void findClosestHigherRegisteredVersionPicksLowestAboveRequested() {
        // Annotation bumped scenario: only newer versions registered, state recorded under the older
        // (now-unregistered) annotation. Pick the lowest registered version strictly greater than state.
        QualifiedName eventName = new QualifiedName("com.example.OrderPlaced");
        registry.register(eventName, new VersionedStub("OrderWorkflow", "0.0.2"));
        registry.register(eventName, new VersionedStub("OrderWorkflow", "0.0.5"));

        var match = registry.findClosestHigherRegisteredVersion("OrderWorkflow", "0.0.1");
        assertThat(match).isPresent();
        assertThat(match.get().workflowVersion()).isEqualTo("0.0.2");
    }

    @Test
    void findClosestHigherRegisteredVersionReturnsEmptyWhenNothingIsAboveRequested() {
        // All registered versions <= state - closest-higher has nothing to offer (caller falls back
        // to closest-down or the WARN fallback).
        QualifiedName eventName = new QualifiedName("com.example.OrderPlaced");
        registry.register(eventName, new VersionedStub("OrderWorkflow", "1.0.0"));
        registry.register(eventName, new VersionedStub("OrderWorkflow", "2.0.0"));

        var match = registry.findClosestHigherRegisteredVersion("OrderWorkflow", "2.0.1");
        assertThat(match).isEmpty();
    }

    @Test
    void testRegisterWithNullInputs() {
        WorkflowConfiguration<?> workflowConfiguration = new StubWorkflowConfiguration();
        assertThatThrownBy(() -> registry.register((EventCondition) null, workflowConfiguration))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> registry.register(EventConditions.never(), null))
                .isInstanceOf(NullPointerException.class);
    }

    /**
     * Stub with explicit workflowName + workflowVersion for {@code findClosestRegisteredVersion} tests.
     */
    private static class VersionedStub extends StubWorkflowConfiguration {

        private final String name;
        private final String version;

        VersionedStub(String name, String version) {
            super();
            this.name = name;
            this.version = version;
        }

        @Override
        public String workflowName() {
            return name;
        }

        @Override
        public String workflowVersion() {
            return version;
        }
    }

    private static EventCondition eventCondition(
            QualifiedName eventName,
            BiPredicate<EventMessage, ProcessingContext> predicate
    ) {
        return new EventCondition() {
            @Override
            public BiPredicate<EventMessage, ProcessingContext> predicate() {
                return predicate;
            }

            @Override
            public QualifiedName qualifiedName() {
                return eventName;
            }
        };
    }

    private static class StubWorkflowConfiguration implements WorkflowConfiguration<WorkflowContext> {

        private final WorkflowDefinition<WorkflowContext> definition;

        public StubWorkflowConfiguration() {
            this(null);
        }

        public StubWorkflowConfiguration(WorkflowDefinition<WorkflowContext> definition) {
            this.definition = definition;
        }

        @Override
        public Class<WorkflowContext> getWorkflowContextType() {
            return WorkflowContext.class;
        }

        @Override
        public WorkflowDefinition<WorkflowContext> workflowDefinition() {
            return definition;
        }

        @Override
        public WorkflowContextFactory<WorkflowContext> workflowContextFactory() {
            return new WorkflowContextFactory<>() {
                @Override
                public WorkflowContext createContext(Map<String, @Nullable Object> initialPayload,
                                                              String workflowId,
                                                              ProcessingContext processingContext,
                                                              WorkflowConfiguration<?> workflowConfiguration) {
                    throw new UnsupportedOperationException("Stub factory can't create contexts");
                }
            };
        }

        @Override
        public WorkflowExecutionFactory workflowExecutionFactory() {
            return new DSLAdoptingExecutionFactory<>(getWorkflowContextType());
        }

        @Override
        public WorkflowIdProvider workflowIdProvider() {
            return new MessageWorkflowIdProvider();
        }

        @Override
        public String workflowName() {
            return WorkflowConfiguration.super.workflowName();
        }

        @Override
        public EventNameCustomizer eventNameCustomizer() {
            return defaults();
        }

        @Override
        public Map<WorkflowStatus, WorkflowStatusChangeListener> workflowStatusChangeListeners() {
            return WorkflowConfiguration.super.workflowStatusChangeListeners();
        }
    }
}
