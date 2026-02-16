package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DefaultEventNameCustomizerTest {

    @Nested
    class BuilderTests {
        @Test
        void testEventName() {
            assertNotNull(DefaultEventNameCustomizer.Builder.eventName());
        }

        @Test
        void testBaseName() {
            var customizer = DefaultEventNameCustomizer.Builder.baseName("MyBase");
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertEquals("MyBaseCompleted", name.localName());
        }

        @Test
        void testNamespace() {
            var customizer = DefaultEventNameCustomizer.Builder.namespace("my.ns");
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertEquals("my.ns", name.namespace());
        }

        @Test
        void testStepCompleted() {
            var customizer = DefaultEventNameCustomizer.Builder.stepCompleted("Done");
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertEquals("StepDone", name.localName());
        }

        @Test
        void testStepStarted() {
            var customizer = DefaultEventNameCustomizer.Builder.stepStarted("Beginning");
            var name = customizer.getEventName("step", Map.of(), StepStatus.STARTED);
            assertEquals("StepBeginning", name.localName());
        }

        @Test
        void testStepFailed() {
            var customizer = DefaultEventNameCustomizer.Builder.stepFailed("Broken");
            var name = customizer.getEventName("step", Map.of(), StepStatus.FAILED);
            assertEquals("StepBroken", name.localName());
        }

        @Test
        void testStepTimedOut() {
            var customizer = DefaultEventNameCustomizer.Builder.stepTimedOut("Late");
            var name = customizer.getEventName("step", Map.of(), StepStatus.TIMED_OUT);
            assertEquals("StepLate", name.localName());
        }

        @Test
        void testAppendToBaseName() {
            var customizer = DefaultEventNameCustomizer.Builder.appendToBaseName(false);
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertEquals("Completed", name.localName());
        }

        @Test
        void testCapitalizeSimpleName() {
            var customizer = DefaultEventNameCustomizer.Builder.capitalizeSimpleName(false);
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertEquals("stepCompleted", name.localName());
        }

        @Test
        void testPayloadCustomization() {
            var customizer = DefaultEventNameCustomizer.Builder.payloadCustomization(pc -> new QualifiedName("custom", "name"));
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertEquals("custom", name.namespace());
            assertEquals("name", name.localName());
        }

        @Test
        void testWorkflowCompleted() {
            var customizer = DefaultEventNameCustomizer.Builder.eventName().workflowCompleted("Finished");
            assertEquals("StepFinished", customizer.getEventName("step", Map.of(), WorkflowStatus.COMPLETED).localName());
        }

        @Test
        void testWorkflowStarted() {
            var customizer = DefaultEventNameCustomizer.Builder.eventName().workflowStarted("Start");
            assertEquals("StepStart", customizer.getEventName("step", Map.of(), WorkflowStatus.STARTED).localName());
        }

        @Test
        void testWorkflowTimedOut() {
            var customizer = DefaultEventNameCustomizer.Builder.eventName().workflowTimedOut("Timeout");
            assertEquals("StepTimeout", customizer.getEventName("step", Map.of(), WorkflowStatus.TIMED_OUT).localName());
        }

        @Test
        void testWorkflowFailed() {
            var customizer = DefaultEventNameCustomizer.Builder.eventName().workflowFailed("Fail");
            assertEquals("StepFail", customizer.getEventName("step", Map.of(), WorkflowStatus.FAILED).localName());
        }
    }

    @Nested
    class NonStaticMethodsTests {
        private final DefaultEventNameCustomizer customizer = new DefaultEventNameCustomizer();

        @Test
        void testBaseName() {
            customizer.baseName("Base");
            assertEquals("BaseCompleted", customizer.getEventName("step", Map.of(), StepStatus.COMPLETED).localName());
        }

        @Test
        void testNamespace() {
            customizer.namespace("ns");
            assertEquals("ns", customizer.getEventName("step", Map.of(), StepStatus.COMPLETED).namespace());
        }

        @Test
        void testAppendToBaseName() {
            customizer.appendToBaseName(false);
            assertEquals("Completed", customizer.getEventName("step", Map.of(), StepStatus.COMPLETED).localName());
        }

        @Test
        void testCapitalizeSimpleName() {
            customizer.capitalizeSimpleName(false);
            assertEquals("stepCompleted", customizer.getEventName("step", Map.of(), StepStatus.COMPLETED).localName());
        }

        @Test
        void testStepCompleted() {
            customizer.stepCompleted("Finished");
            assertEquals("StepFinished", customizer.getEventName("step", Map.of(), StepStatus.COMPLETED).localName());
        }

        @Test
        void testStepStarted() {
            customizer.stepStarted("Start");
            assertEquals("StepStart", customizer.getEventName("step", Map.of(), StepStatus.STARTED).localName());
        }

        @Test
        void testStepTimedOut() {
            customizer.stepTimedOut("Timeout");
            assertEquals("StepTimeout", customizer.getEventName("step", Map.of(), StepStatus.TIMED_OUT).localName());
        }

        @Test
        void testStepFailed() {
            customizer.stepFailed("Fail");
            assertEquals("StepFail", customizer.getEventName("step", Map.of(), StepStatus.FAILED).localName());
        }

        @Test
        void testWorkflowCompleted() {
            customizer.workflowCompleted("Finished");
            assertEquals("StepFinished", customizer.getEventName("step", Map.of(), WorkflowStatus.COMPLETED).localName());
        }

        @Test
        void testWorkflowStarted() {
            customizer.workflowStarted("Start");
            assertEquals("StepStart", customizer.getEventName("step", Map.of(), WorkflowStatus.STARTED).localName());
        }

        @Test
        void testWorkflowTimedOut() {
            customizer.workflowTimedOut("Timeout");
            assertEquals("StepTimeout", customizer.getEventName("step", Map.of(), WorkflowStatus.TIMED_OUT).localName());
        }

        @Test
        void testWorkflowFailed() {
            customizer.workflowFailed("Fail");
            assertEquals("StepFail", customizer.getEventName("step", Map.of(), WorkflowStatus.FAILED).localName());
        }

        @Test
        void testPayloadCustomization() {
            customizer.payloadCustomization(pc -> new QualifiedName("p", "c"));
            var name = customizer.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertEquals("p", name.namespace());
            assertEquals("c", name.localName());
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
            assertEquals("parent.ns", name.namespace());
            assertEquals("ChildCompleted", name.localName());
        }

        @Test
        void testMergeWorkflowStatus() {
            var parent = DefaultEventNameCustomizer.Builder.eventName().namespace("parent.ns").workflowCompleted("ParentDone");
            var child = DefaultEventNameCustomizer.Builder.eventName().workflowCompleted("ChildDone");
            var merged = DefaultEventNameCustomizer.Builder.merge(parent, child);

            var name = merged.getEventName("step", Map.of(), WorkflowStatus.COMPLETED);
            assertEquals("parent.ns", name.namespace());
            assertEquals("StepChildDone", name.localName());
        }
    }

    @Nested
    class ForStepInheritanceTests {

        @Test
        void propagatesNamespace() {
            var parent = DefaultEventNameCustomizer.Builder.namespace("my.ns");
            var inherited = parent.forStepInheritance();
            var name = inherited.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertEquals("my.ns", name.namespace());
        }

        @Test
        void propagatesCapitalizeSimpleName() {
            var parent = DefaultEventNameCustomizer.Builder.capitalizeSimpleName(false);
            var inherited = parent.forStepInheritance();
            var name = inherited.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertEquals("stepCompleted", name.localName());
        }

        @Test
        void doesNotPropagateAppendToBaseName() {
            var parent = DefaultEventNameCustomizer.Builder.appendToBaseName(false);
            var inherited = parent.forStepInheritance();
            var name = inherited.getEventName("step", Map.of(), StepStatus.COMPLETED);
            // appendToBaseName resets to true (default), so step name is included
            assertEquals("StepCompleted", name.localName());
        }

        @Test
        void propagatesStepStatusNames() {
            var parent = DefaultEventNameCustomizer.Builder.stepCompleted("Done");
            var inherited = parent.forStepInheritance();
            var name = inherited.getEventName("step", Map.of(), StepStatus.COMPLETED);
            assertEquals("StepDone", name.localName());
        }

        @Test
        void doesNotPropagateBaseName() {
            var parent = DefaultEventNameCustomizer.Builder.baseName("Order");
            var inherited = parent.forStepInheritance();
            var name = inherited.getEventName("myStep", Map.of(), StepStatus.COMPLETED);
            // Should use the step name "myStep" instead of parent's baseName "Order"
            assertEquals("MyStepCompleted", name.localName());
        }

        @Test
        void doesNotPropagatePayloadCustomization() {
            var parent = DefaultEventNameCustomizer.Builder.payloadCustomization(
                pc -> new QualifiedName("custom.ns", "CustomName")
            );
            var inherited = parent.forStepInheritance();
            var name = inherited.getEventName("step", Map.of(), StepStatus.COMPLETED);
            // Should use default behavior, not the custom payload function
            assertEquals("io.axoniq.workflow", name.namespace());
            assertEquals("StepCompleted", name.localName());
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
            assertEquals("StepCompleted", nameNormal.localName());

            var nameCustom = customizer.getEventName("step", Map.of("useCustom", true), StepStatus.COMPLETED);
            assertEquals("CustomStepCompleted", nameCustom.localName());
        }
    }
}
