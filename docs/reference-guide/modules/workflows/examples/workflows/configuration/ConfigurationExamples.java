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

package workflows.configuration;

import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.correlation.MessageOriginProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.equalsTo;
import static io.axoniq.framework.workflow.dsl.api.EventAssociationsUtils.payloadProperty;
import static io.axoniq.framework.workflow.runtime.association.Associations.associate;
import static io.axoniq.framework.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.framework.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;

public class ConfigurationExamples {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurationExamples.class);

    public void fullExample() {
        // tag::full-example[]
        var configurer = WorkflowConfigurer.create();

        configurer.registerWorkflowModule(
                WorkflowModule.defaults("order-workflows", SimpleWorkflowContext.class)
                              .definition(d -> d
                                      .declarative(c -> new OrderFulfillmentWorkflow()::execute)
                                      .workflowName("OrderFulfillment")
                                      .on(EventConditions.fromType(OrderPlacedEvent.class))
                                      .customized((c, w) -> w
                                              .eventNameCustomizer(namespace("io.myapp.orders"))
                                              .workflowIdProvider(new PayloadPropertyWorkflowIdProvider("orderId"))
                                      )
                              )
        );

        var configuration = configurer.start(); // <1>
        // end::full-example[]
    }

    public void step1ModuleAndContextType() {
        // tag::step1-module-context-type[]
        WorkflowModule.defaults("order-workflows", SimpleWorkflowContext.class)
                      // end::step1-module-context-type[]
                      .definition(d -> d.autodetected(c -> new OrderFulfillmentWorkflow()));
    }

    public void step3ContextFactory() {
        WorkflowModule.defaults("order-workflows", SimpleWorkflowContext.class)
                      .definition(d -> d.autodetected(c -> new OrderFulfillmentWorkflow()))
                      // tag::step3-context-factory[]
                      .contextFactory(c -> new SimpleWorkflowContextFactory())
        // end::step3-context-factory[]
        ;
    }

    public void step2DeclarativeDefinition() {
        var workflow = new OrderFulfillmentWorkflow();

        WorkflowModule.defaults("order-workflows", SimpleWorkflowContext.class)
                      // tag::step2-declarative-definition[]
                      .definition(d -> d
                              .declarative(c -> workflow::execute)
                              .workflowName("OrderFulfillment")
                              .on(EventConditions.fromType(OrderPlacedEvent.class))
                              .customized((c, w) -> w /* ... */)
                      );
        // end::step2-declarative-definition[]
    }

    public void step2AutodetectedDefinition() {
        // tag::step2-autodetected-definition[]
        WorkflowModule.defaults("order-workflows", SimpleWorkflowContext.class)
                      .definition(d -> d
                              .autodetected(c -> new OrderFulfillmentWorkflow())
                      );
        // end::step2-autodetected-definition[]
    }

    public void multipleDefinitions() {
        var workflowA = new WorkflowA();
        var workflowB = new WorkflowB();

        // tag::multiple-definitions[]
        WorkflowModule.defaults("my-workflows", SimpleWorkflowContext.class)
                      .definition(d -> d
                              .declarative(c -> workflowA::execute)
                              .workflowName("WorkflowA")
                              .on(EventConditions.fromType(EventA.class))
                              .notCustomized())
                      .definition(d -> d
                              .declarative(c -> workflowB::execute)
                              .workflowName("WorkflowB")
                              .on(EventConditions.fromType(EventB.class))
                              .notCustomized());
        // end::multiple-definitions[]
    }

    public void startConditionsVip() {
        var workflow = new OrderFulfillmentWorkflow();

        WorkflowModule.defaults("order-workflows", SimpleWorkflowContext.class)
                      .definition(d -> d
                              .declarative(c -> workflow::execute)
                              .workflowName("OrderFulfillment")
                              // tag::start-conditions-vip[]
                              .on(EventConditions.fromType(
                                      RegistrationReceivedEvent.class,
                                      associate(payloadProperty("status"), equalsTo("vip"))
                              ))
                              // end::start-conditions-vip[]
                              .notCustomized());
    }

    public void workflowIdProvider(Configuration c) {
        var workflow = new OrderFulfillmentWorkflow();

        WorkflowModule.defaults("order-workflows", SimpleWorkflowContext.class)
                      .definition(d -> d
                              .declarative(c2 -> workflow::execute)
                              .workflowName("OrderFulfillment")
                              .on(EventConditions.fromType(OrderPlacedEvent.class))
                              .customized((c2, w) -> w
                                                  // tag::workflow-id-provider[]
                                                  // Extract orderId from payload, prefix it
                                                  .workflowIdProvider(fromPayloadAttribute(c, "orderId", id -> "order-" + id))

                                                  // Extract directly without transformation
                                                  .workflowIdProvider(new PayloadPropertyWorkflowIdProvider("orderId"))
                                      // end::workflow-id-provider[]
                              ));
    }

    public void customizationOptions() {
        var workflow = new OrderFulfillmentWorkflow();

        WorkflowModule.defaults("order-workflows", SimpleWorkflowContext.class).definition(
                d -> d.declarative(c -> workflow::execute)
                      .workflowName("OrderFulfillment")
                      .on(EventConditions.fromType(OrderPlacedEvent.class))
                      // tag::customization-options[]
                      .customized((c, w) -> w
                              .eventNameCustomizer(
                                      namespace("io.myapp.orders")
                                              .stepCompleted("Done")
                                              .workflowBaseName("OrderFulfillment")
                              )
                              .workflowIdProvider(new PayloadPropertyWorkflowIdProvider("orderId"))
                              .registerWorkflowStatusChangeListener(
                                      WorkflowStatus.COMPLETED,
                                      (status, context, processingContext) -> logger.info(
                                              "Workflow {} completed", context.workflowId()
                                      )
                              )
                      )
                // end::customization-options[]
        );
    }

    public void processorSettings() {
        // tag::processor-settings[]
        WorkflowModule.configure("order-workflows", SimpleWorkflowContext.class)
                      .definition(d -> d.autodetected(c -> new OrderFulfillmentWorkflow()))
                      .processorConfiguration(processor -> processor.initialSegmentCount(8));
        // end::processor-settings[]
    }

    public void correlationBuiltIn(WorkflowConfigurer configurer) {
        // tag::correlation-built-in[]
        configurer.eventSourcing(es -> es.messaging(
                m -> m.registerCorrelationDataProvider(
                        config -> new MessageOriginProvider()   // <1>
                )
        ));
        // end::correlation-built-in[]
    }

    public void correlationCustom(WorkflowConfigurer configurer) {
        // tag::correlation-custom[]
        configurer.eventSourcing(es -> es.messaging(m -> m
                .registerCorrelationDataProvider(
                        config -> message -> {
                            var metadata = message.metadata();
                            var result = new HashMap<String, String>();
                            if (metadata.containsKey("tenantId")) {
                                result.put("tenantId", metadata.get("tenantId"));
                            }
                            if (metadata.containsKey("userId")) {
                                result.put("userId", metadata.get("userId"));
                            }
                            return result;
                        }
                )
        ));
        // end::correlation-custom[]
    }

    public void customContextWiring() {
        // tag::custom-context-wiring[]
        WorkflowModule.defaults("purchase-approval", ApprovalWorkflowContext.class)
                      .definition(d -> d
                              .declarative(c -> new PurchaseApprovalWorkflow()::execute)
                              .workflowName("PurchaseApproval")
                              .on(EventConditions.fromType(PurchaseRequestSubmitted.class))
                              .notCustomized()
                      )
                      .contextFactory(c -> new ApprovalWorkflowContextFactory());
        // end::custom-context-wiring[]
    }
}
