/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package workflows.errorhandling;

import io.axoniq.framework.workflow.configuration.WorkflowConfigurer;
import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.runtime.api.execution.context.RecoverableWorkflowExceptionPolicy;
import org.springframework.context.annotation.Bean;

public class ErrorHandlingConfigurationExamples {

    public void registerPolicyComponent() {
        var configurer = WorkflowConfigurer.create();

        // tag::register-policy-component[]
        configurer.componentRegistry(registry -> registry.registerComponent(
                RecoverableWorkflowExceptionPolicy.class,
                c -> RecoverableWorkflowExceptionPolicy.DEFAULT.or(
                        e -> e instanceof BackendUnavailableException
                )
        ));
        // end::register-policy-component[]
    }

    // tag::spring-bean-policy[]
    @Bean
    public RecoverableWorkflowExceptionPolicy recoverableExceptionPolicy() {
        return RecoverableWorkflowExceptionPolicy.DEFAULT.or(
                e -> e instanceof BackendUnavailableException
        );
    }
    // end::spring-bean-policy[]

    public void customizedPolicy() {
        var workflow = new OrderFulfillmentWorkflow();

        WorkflowModule.defaults("order-workflows", SimpleWorkflowContext.class).definition(
                d -> d.declarative(c -> workflow::execute)
                      .workflowName("OrderFulfillment")
                      .on(EventConditions.fromType(OrderPlacedEvent.class))
                      // tag::customized-policy[]
                      .customized((c, w) -> w
                              .recoverableExceptionPolicy(
                                      RecoverableWorkflowExceptionPolicy.DEFAULT.or(
                                              e -> e instanceof BackendUnavailableException
                                      )
                              )
                      )
                // end::customized-policy[]
        );
    }
}
