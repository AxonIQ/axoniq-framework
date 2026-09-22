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
package io.axoniq.framework.workflow;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.library.DependencyRules;
import io.axoniq.framework.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.framework.workflow.runtime.util.EventMessageUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;
import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.merge;
import static io.axoniq.framework.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer.NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@AnalyzeClasses(
        packages = {"io.axoniq.framework.workflow"}
)
public class WorkflowArchUnitConventionTests
        // TODO#115 as part of the implementation of https://github.com/AxonIQ/extension-workflow/issues/115 uncomment the following line
//        implements MainArchUnitConventions
{

    @ArchTest
    private final ArchRule packagesShouldBeFreeOfCycles =
            slices()
                    .matching("(**)")
                    .should()
                    .beFreeOfCycles()
                    .as("Package Cycles");

    @ArchTest
    private final ArchRule noClassesShouldDependOnUpperPackages =
            DependencyRules.NO_CLASSES_SHOULD_DEPEND_UPPER_PACKAGES
                    .as("Package Hierarchy Violations");

  /**
   * Verifies that step events inherit the workflow namespace by default and that a step override wins when provided.
   */
  static class WorkflowEventNamespaceInheritanceTest {

      @Test
      void stepEventsInheritWorkflowNamespaceByDefault() {
          // Given a workflow-level namespace
          EventNameCustomizer workflowCustomizer = DefaultEventNameCustomizer.Builder.namespace("wf.ns");
          // And a step-level customizer which is not overriding anything (defaults)
          EventNameCustomizer stepCustomizer = DefaultEventNameCustomizer.Builder.defaults();
          EventNameCustomizer merged = merge(workflowCustomizer, stepCustomizer);

          WorkflowContext ctx = mock(WorkflowContext.class);
          when(ctx.workflowId()).thenReturn("wf-1");
          when(ctx.workflowPayload()).thenReturn(Map.of());
          ProcessingContext processingContext = mock(ProcessingContext.class);
          when(ctx.processingContext()).thenReturn(processingContext);
          when(processingContext.component(EventConverter.class)).thenReturn(mock(EventConverter.class));

          // When producing a completed step event using the merged customizer
          EventMessage evt = EventMessageUtils.completedStep(ctx,
                                                             "myStep",
                                                             Map.of(),
            NAME,
                                                             merged);

          // Then the namespace should be inherited from the workflow customizer
          assertThat(evt.type().qualifiedName().namespace()).isEqualTo("wf.ns");
          assertThat(evt.type().qualifiedName().localName()).isEqualTo("MyStepCompleted");
      }

      @Test
      void stepNamespaceOverrideWinsOverWorkflowNamespace() {
          // Given a workflow-level namespace
          EventNameCustomizer workflowCustomizer = DefaultEventNameCustomizer.Builder.namespace("wf.ns");
          // And a step-level override for namespace
          EventNameCustomizer stepCustomizer = DefaultEventNameCustomizer.Builder.namespace("step.ns");
          EventNameCustomizer merged = merge(workflowCustomizer, stepCustomizer);

          WorkflowContext ctx = mock(WorkflowContext.class);
          when(ctx.workflowId()).thenReturn("wf-1");
          when(ctx.workflowPayload()).thenReturn(Map.of());
          ProcessingContext processingContext = mock(ProcessingContext.class);
          when(ctx.processingContext()).thenReturn(processingContext);
          when(processingContext.component(EventConverter.class)).thenReturn(mock(EventConverter.class));

          // When producing a completed step event using the merged customizer
          EventMessage evt = EventMessageUtils.completedStep(ctx,
                                                             "myStep",
                                                             Map.of(),
            NAME,
                                                             merged);

          // Then the step-level namespace should win
          assertThat(evt.type().qualifiedName().namespace()).isEqualTo("step.ns");
      }
  }
}
