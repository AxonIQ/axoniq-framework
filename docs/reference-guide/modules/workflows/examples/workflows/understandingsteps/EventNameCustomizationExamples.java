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

package workflows.understandingsteps;

import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import org.axonframework.messaging.core.QualifiedName;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;

import static io.axoniq.framework.workflow.dsl.api.Payload.payload;
import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.*;

public class EventNameCustomizationExamples {

    private Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>,
            WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> defineWorkflow(
            OrderFulfillmentWorkflow workflow
    ) {
        // tag::declarative-customizer[]
        return d -> d.declarative(c -> workflow::execute)
                     .workflowName("OrderFulfillment")
                     .on(EventConditions.fromType(OrderPlacedEvent.class))
                     .customized((c, w) -> w
                             .eventNameCustomizer(
                                     namespace("io.myapp.orders")
                                             .stepCompleted("Done")
                                             .workflowBaseName("OrderFulfillment"))
                             .workflowIdProvider(new PayloadPropertyWorkflowIdProvider("orderId"))
                     );
        // end::declarative-customizer[]
    }

    public void stepLevelCustomizerExample(
            SimpleWorkflowContext context,
            double amount
    ) {
        // tag::step-level-customizer-example[]
        context.awaitExecute(
                "initiatePayment",
                payload("amount", amount).getValues(),
                PaymentService::initiatePayment,
                step -> step.timeout(Duration.ofSeconds(30))
                            .eventNameCustomizer(namespace("io.myapp.payments")) // <1>
        );
        // end::step-level-customizer-example[]
    }

    public void stepLevelCancelledNamespace(SimpleWorkflowContext context) {
        // tag::step-level-cancelled-namespace[]
        context.execute(
                "shipOrder",
                context.workflowPayload(),
                ShippingService::shipOrder,
                step -> step.timeout(Duration.ofMinutes(5))
                            .eventNameCustomizer(namespace("io.shipping"))
        );

        // If cancelled, the event will be: io.shipping.ShipOrderCancelled
        // end::step-level-cancelled-namespace[]
    }

    public void stepCancelledSuffix() {
        // tag::step-cancelled-suffix[]
        defaults().stepCancelled("Aborted")
        // produces: io.namespace.ShipOrderAborted
        // end::step-cancelled-suffix[]
        ;
    }

    public void workflowLevelDoneSuffix(OrderFulfillmentWorkflow workflow) {
        WorkflowModule.defaults("order-workflows", SimpleWorkflowContext.class).definition(
                d -> d.declarative(c -> workflow::execute)
                      .workflowName("OrderFulfillment")
                      .on(EventConditions.fromType(OrderPlacedEvent.class))
                      // tag::workflow-level-done-suffix[]
                      // Workflow-level: all steps use io.myapp namespace and "Done" suffix
                      .customized((c, w) -> w.eventNameCustomizer(
                              namespace("io.myapp").stepCompleted("Done")
                      ))
                // end::workflow-level-done-suffix[]
        );
    }

    public void twoStepNamespaceExample(
            SimpleWorkflowContext context,
            double amount
    ) {
        // tag::two-step-namespace-example[]
        // Step 1: inherits workflow customizer, no override needed
        context.awaitExecute(
                "reserveStock",
                Map.of(),
                (pc, input) -> Map.of("reserved", InventoryService.reserveStock())
        );

        // Step 2: overrides namespace for this step only
        context.awaitExecute(
                "initiatePayment",
                payload("amount", amount).getValues(),
                PaymentService::initiatePayment,
                step -> step.timeout(Duration.ofSeconds(30))
                            .eventNameCustomizer(namespace("io.payments"))
        );
        // end::two-step-namespace-example[]
    }

    public void builderChainingExample() {
        // tag::builder-chaining-example[]
        namespace("io.myapp").stepCompleted("Done")
                             .stepFailed("Errored")
        // end::builder-chaining-example[]
        ;
    }

    public void customizingSuffixesExample() {
        // tag::customizing-suffixes-example[]
        defaults().stepStarted("Initiated")
                  .stepCompleted("Done")
                  .stepFailed("Errored")
                  .stepTimedOut("Expired")
                  .stepCancelled("Aborted")
        // end::customizing-suffixes-example[]
        ;
    }

    public void payloadCustomizationExample() {
        // tag::payload-customization-example[]
        payloadCustomization(pc -> {
            var priority = pc.payload().getOrDefault("priority", "normal");
            return new QualifiedName(pc.namespaceTemplate(),
                                     priority + pc.localNameTemplate());
        })
        // end::payload-customization-example[]
        ;
    }
}
