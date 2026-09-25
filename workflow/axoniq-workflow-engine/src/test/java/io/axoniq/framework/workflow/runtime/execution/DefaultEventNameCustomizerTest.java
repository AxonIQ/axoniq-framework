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

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link DefaultEventNameCustomizer}.
 */
class DefaultEventNameCustomizerTest {

    @Nested
    class BuilderTests {

        @Test
        void testDefaults() {
            assertThat(DefaultEventNameCustomizer.Builder.defaults()).isNotNull();
        }

        @Test
        void testBaseName() {
            var customizer = DefaultEventNameCustomizer.Builder.baseName("MyBase");
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertThat(name.localName()).isEqualTo("MyBaseCompleted");
        }

        @Test
        void testNamespace() {
            var customizer = DefaultEventNameCustomizer.Builder.namespace("my.ns");
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertThat(name.namespace()).isEqualTo("my.ns");
        }

        @Test
        void testStepCompleted() {
            var customizer = DefaultEventNameCustomizer.Builder.stepCompleted("Done");
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertThat(name.localName()).isEqualTo("StepDone");
        }

        @Test
        void testStepStarted() {
            var customizer = DefaultEventNameCustomizer.Builder.stepStarted("Beginning");
            var name = customizer.getEventName("step", Map.of(), StepStatus.STARTED);
            assertThat(name.localName()).isEqualTo("StepBeginning");
        }

        @Test
        void testStepFailed() {
            var customizer = DefaultEventNameCustomizer.Builder.stepFailed("Broken");
            var name = customizer.getEventName("step", Map.of(), StepStatus.FAILED);
            assertThat(name.localName()).isEqualTo("StepBroken");
        }

        @Test
        void testStepTimedOut() {
            var customizer = DefaultEventNameCustomizer.Builder.stepTimedOut("Late");
            var name = customizer.getEventName("step", Map.of(), StepStatus.TIMED_OUT);
            assertThat(name.localName()).isEqualTo("StepLate");
        }

        @Test
        void testAppendToBaseName() {
            var customizer = DefaultEventNameCustomizer.Builder.appendToBaseName(false);
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertThat(name.localName()).isEqualTo("Completed");
        }

        @Test
        void testCapitalizeSimpleName() {
            var customizer = DefaultEventNameCustomizer.Builder.capitalizeSimpleName(false);
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertThat(name.localName()).isEqualTo("stepCompleted");
        }

        @Test
        void testPayloadCustomization() {
            var customizer = DefaultEventNameCustomizer.Builder.payloadCustomization(pc -> new QualifiedName("custom",
                                                                                                             "name"));
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertThat(name.namespace()).isEqualTo("custom");
            assertThat(name.localName()).isEqualTo("name");
        }

        @Test
        void testWorkflowCompleted() {
            var customizer = DefaultEventNameCustomizer.Builder.defaults().workflowCompleted("Finished");
            assertThat(customizer.getEventName("step", Map.of(), WorkflowStatus.COMPLETED).localName()).isEqualTo(
                    "StepFinished");
        }

        @Test
        void testWorkflowStarted() {
            var customizer = DefaultEventNameCustomizer.Builder.defaults().workflowStarted("Start");
            assertThat(customizer.getEventName("step", Map.of(), WorkflowStatus.STARTED).localName()).isEqualTo(
                    "StepStart");
        }

        @Test
        void testWorkflowTimedOut() {
            var customizer = DefaultEventNameCustomizer.Builder.defaults().workflowTimedOut("Timeout");
            assertThat(customizer.getEventName("step", Map.of(), WorkflowStatus.TIMED_OUT).localName()).isEqualTo(
                    "StepTimeout");
        }

        @Test
        void testWorkflowFailed() {
            var customizer = DefaultEventNameCustomizer.Builder.defaults().workflowFailed("Fail");
            assertThat(customizer.getEventName("step", Map.of(), WorkflowStatus.FAILED).localName()).isEqualTo(
                    "StepFail");
        }
    }

    @Nested
    class NonStaticMethodsTests {

        private final DefaultEventNameCustomizer customizer = new DefaultEventNameCustomizer();

        @Test
        void testBaseName() {
            customizer.baseName("Base");
            assertThat(customizer.getEventName("step", Map.of(), StepStatus.COMPLETED).localName()).isEqualTo(
                    "BaseCompleted");
        }

        @Test
        void testNamespace() {
            customizer.namespace("ns");
            assertThat(customizer.getEventName("step", Map.of(), StepStatus.COMPLETED).namespace()).isEqualTo("ns");
        }

        @Test
        void testAppendToBaseName() {
            customizer.appendToBaseName(false);
            assertThat(customizer.getEventName("step", Map.of(), StepStatus.COMPLETED).localName()).isEqualTo(
                    "Completed");
        }

        @Test
        void testCapitalizeSimpleName() {
            customizer.capitalizeSimpleName(false);
            assertThat(customizer.getEventName("step", Map.of(), StepStatus.COMPLETED).localName()).isEqualTo(
                    "stepCompleted");
        }

        @Test
        void testStepCompleted() {
            customizer.stepCompleted("Finished");
            assertThat(customizer.getEventName("step", Map.of(), StepStatus.COMPLETED).localName()).isEqualTo(
                    "StepFinished");
        }

        @Test
        void testStepStarted() {
            customizer.stepStarted("Start");
            assertThat(customizer.getEventName("step", Map.of(), StepStatus.STARTED)
                                 .localName()).isEqualTo("StepStart");
        }

        @Test
        void testStepRetryStartedDefaultsToRetryStarted() {
            assertThat(customizer.getEventName("step", Map.of(), StepStatus.RETRY_STARTED).localName())
                    .isEqualTo("StepRetryStarted");
        }

        @Test
        void testStepRetryStarted() {
            customizer.stepRetryStarted("Reattempt");
            assertThat(customizer.getEventName("step", Map.of(), StepStatus.RETRY_STARTED).localName())
                    .isEqualTo("StepReattempt");
        }

        @Test
        void testStepTimedOut() {
            customizer.stepTimedOut("Timeout");
            assertThat(customizer.getEventName("step", Map.of(), StepStatus.TIMED_OUT).localName()).isEqualTo(
                    "StepTimeout");
        }

        @Test
        void testStepFailed() {
            customizer.stepFailed("Fail");
            assertThat(customizer.getEventName("step", Map.of(), StepStatus.FAILED).localName()).isEqualTo("StepFail");
        }

        @Test
        void testWorkflowCompleted() {
            customizer.workflowCompleted("Finished");
            assertThat(customizer.getEventName("step", Map.of(), WorkflowStatus.COMPLETED).localName()).isEqualTo(
                    "StepFinished");
        }

        @Test
        void testWorkflowStarted() {
            customizer.workflowStarted("Start");
            assertThat(customizer.getEventName("step", Map.of(), WorkflowStatus.STARTED).localName()).isEqualTo(
                    "StepStart");
        }

        @Test
        void testWorkflowTimedOut() {
            customizer.workflowTimedOut("Timeout");
            assertThat(customizer.getEventName("step", Map.of(), WorkflowStatus.TIMED_OUT).localName()).isEqualTo(
                    "StepTimeout");
        }

        @Test
        void testWorkflowFailed() {
            customizer.workflowFailed("Fail");
            assertThat(customizer.getEventName("step", Map.of(), WorkflowStatus.FAILED).localName()).isEqualTo(
                    "StepFail");
        }

        @Test
        void testPayloadCustomization() {
            customizer.payloadCustomization(pc -> new QualifiedName("p", "c"));
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertThat(name.namespace()).isEqualTo("p");
            assertThat(name.localName()).isEqualTo("c");
        }
    }

    @Nested
    class MergeTests {

        @Test
        void testMergeStepStatus() {
            var parent = DefaultEventNameCustomizer.Builder.namespace("parent.ns").baseName("Parent");
            var child = DefaultEventNameCustomizer.Builder.baseName("Child");
            var merged = DefaultEventNameCustomizer.Builder.merge(parent, child);

            var name = merged.getEventName("step", Map.of(), StepStatus.COMPLETED);
            // Child baseName should win, Parent namespace should stay
            assertThat(name.namespace()).isEqualTo("parent.ns");
            assertThat(name.localName()).isEqualTo("ChildCompleted");
        }

        @Test
        void testMergeStepStatusWithDefaultChild() {
            var parent = DefaultEventNameCustomizer.Builder.namespace("parent.ns").baseName("Parent");
            var child = DefaultEventNameCustomizer.Builder.defaults(); // Default namespace "io.axoniq.framework.workflow"
            var merged = DefaultEventNameCustomizer.Builder.merge(parent, child);

            var name = merged.getEventName("step", Map.of(), StepStatus.COMPLETED);
            // Child is default, so Parent namespace should win
            assertThat(name.namespace()).isEqualTo("parent.ns");
            assertThat(name.localName()).isEqualTo("ParentCompleted");
        }

        @Test
        void testMergeWithOnlyNamespaceOnParent() {
            var parent = DefaultEventNameCustomizer.Builder.namespace("parent.ns");
            var child = DefaultEventNameCustomizer.Builder.defaults();
            var merged = DefaultEventNameCustomizer.Builder.merge(parent, child);

            var name = merged.getEventName("step", Map.of(), StepStatus.COMPLETED);
            // Parent namespace should win
            assertThat(name.namespace()).isEqualTo("parent.ns");
            assertThat(name.localName()).isEqualTo("StepCompleted");
        }

        @Test
        void testMergeWorkflowStatus() {
            var parent = DefaultEventNameCustomizer.Builder.defaults().namespace("parent.ns").workflowCompleted(
                    "ParentDone");
            var child = DefaultEventNameCustomizer.Builder.defaults().workflowCompleted("ChildDone");
            var merged = DefaultEventNameCustomizer.Builder.merge(parent, child);

            var name = merged.getEventName("step", Map.of(), WorkflowStatus.COMPLETED);
            assertThat(name.namespace()).isEqualTo("parent.ns");
            assertThat(name.localName()).isEqualTo("StepChildDone");
        }
    }

    @Nested
    class ForStepInheritanceTests {

        @Test
        void propagatesNamespace() {
            var parent = DefaultEventNameCustomizer.Builder.namespace("my.ns");
            var inherited = parent.forStepInheritance();
            var name = inherited.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertThat(name.namespace()).isEqualTo("my.ns");
        }

        @Test
        void propagatesCapitalizeSimpleName() {
            var parent = DefaultEventNameCustomizer.Builder.capitalizeSimpleName(false);
            var inherited = parent.forStepInheritance();
            var name = inherited.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertThat(name.localName()).isEqualTo("stepCompleted");
        }

        @Test
        void doesNotPropagateAppendToBaseName() {
            var parent = DefaultEventNameCustomizer.Builder.appendToBaseName(false);
            var inherited = parent.forStepInheritance();
            var name = inherited.getEventName("step", Map.of(), StepStatus.COMPLETED);
            // appendToBaseName resets to true (default), so step name is included
            assertThat(name.localName()).isEqualTo("StepCompleted");
        }

        @Test
        void propagatesStepStatusNames() {
            var parent = DefaultEventNameCustomizer.Builder.stepCompleted("Done");
            var inherited = parent.forStepInheritance();
            var name = inherited.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertThat(name.localName()).isEqualTo("StepDone");
        }

        @Test
        void doesNotPropagateBaseName() {
            var parent = DefaultEventNameCustomizer.Builder.baseName("Order");
            var inherited = parent.forStepInheritance();
            var name = inherited.getEventName("myStep", Map.of(), StepStatus.COMPLETED);
            // Should use the step name "myStep" instead of parent's baseName "Order"
            assertThat(name.localName()).isEqualTo("MyStepCompleted");
        }

        @Test
        void doesNotPropagatePayloadCustomization() {
            var parent = DefaultEventNameCustomizer.Builder.payloadCustomization(
                    pc -> new QualifiedName("custom.ns", "CustomName")
            );
            var inherited = parent.forStepInheritance();
            var name = inherited.getEventName("step", Map.of(), StepStatus.COMPLETED);
            // Should use default behavior, not the custom payload function
            assertThat(name.namespace()).isEqualTo("io.axoniq.framework.workflow");
            assertThat(name.localName()).isEqualTo("StepCompleted");
        }
    }

    @Nested
    class PayloadApplicationTests {

        @Test
        void testCustomizationBasedOnPayload() {
            var customizer = DefaultEventNameCustomizer.Builder.payloadCustomization(pc -> {
                String localName = pc.localNameTemplate();
                if (pc.payload().containsKey("useCustom")) {
                    localName = "Custom" + localName;
                }
                return new QualifiedName(pc.namespaceTemplate(), localName);
            });

            var nameNormal = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertThat(nameNormal.localName()).isEqualTo("StepCompleted");

            var nameCustom = customizer.getEventName("step", Map.of("useCustom", true), StepStatus.COMPLETED);
            assertThat(nameCustom.localName()).isEqualTo("CustomStepCompleted");
        }
    }
}
