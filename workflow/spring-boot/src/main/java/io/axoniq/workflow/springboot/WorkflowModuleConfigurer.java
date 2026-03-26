package io.axoniq.workflow.springboot;

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.jspecify.annotations.NonNull;


@Internal
@RegistrationScope("Don't copy this enhancer in order to avoid cyclic module build in Spring Boot.")
public class WorkflowModuleConfigurer implements ConfigurationEnhancer {

    @Override
    public void enhance(@NonNull ComponentRegistry registry) {
        registry.registerEnhancer(
                new WorkflowModuleEnhancer(
                        WorkflowModule
                                .usingContext(SimpleWorkflowContext.class)
                                .workflowContextFactory(c -> new SimpleWorkflowContextFactory())
                                .workflowExecutionFactory(c -> new DSLAdoptingExecutionFactory<>(SimpleWorkflowContext.class))
                                .definitions(
                                        d -> d.autodetected(
                                                c -> new PaymentWorkflow(
                                                        c.getComponent(CommandGateway.class)
                                                ),
                                                SimpleWorkflowContext.class
                                        )
                                )
        )
    }
}
