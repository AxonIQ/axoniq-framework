package io.axoniq.workflow;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.library.DependencyRules;
import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.util.EventMessageUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.merge;
import static io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer.NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@AnalyzeClasses(
        packages = {"io.axoniq.workflow"}
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
