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
package io.axoniq.workflow.configuration;

import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.Module;

import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

/**
 * Workflow module encapsulates configuration for one DSL and multiple definitions created using this DSL.
 *
 * @param <C> workflow context type.
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public interface WorkflowModule<C extends WorkflowContext> extends Module {

    /**
     * Creates a new workflow module using the specified workflow context.
     *
     * @param contextType context class.
     * @param <C>         type of the workflow context.
     * @return module builder.
     */
    static <C extends WorkflowContext> LanguagePhase.WorkflowContextFactoryPhase<C> usingContext(
            @Nonnull Class<C> contextType) {
        return new SimpleWorkflowModule<>(contextType);
    }

    /**
     * Retrieves workflow context type.
     *
     * @return workflow context type.
     */
    Class<C> getContextType();

    /**
     * Defines the DSL part of the workflow definition.
     */
    interface LanguagePhase {

        interface WorkflowContextFactoryPhase<C extends WorkflowContext> {

            /**
             * Provides a workflow context factory.
             *
             * @param workflowContextFactory factory to create a new workflow context.
             * @return builder for the state factory.
             */
            WorkflowStateFactoryPhase<C> workflowContextFactory(
                    @Nonnull ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory);
        }

        interface WorkflowStateFactoryPhase<C extends WorkflowContext> {

            /**
             * Provide a workflow execution factory.
             *
             * @param workflowExecutionFactory factory to create a new workflow execution from the given context.
             * @return builder for workflow definition.
             */
            WorkflowDefinitionPhase<C> workflowExecutionFactory(
                    @Nonnull ComponentBuilder<WorkflowExecutionFactory> workflowExecutionFactory);
        }
    }

    interface WorkflowDefinitionPhase<C extends WorkflowContext> {

        /**
         * Defines workflow definitions.
         *
         * @param definitions definitions phase.
         * @return workflow module.
         */
        WorkflowModule<C> definitions(@Nonnull UnaryOperator<DetectionPhase<C>> definitions);


        interface DetectionPhase<C extends WorkflowContext> {

            /**
             * Names the workflow.
             *
             * @param componentBuilder builder for the workflow component.
             * @return builder for the trigger definition phase.
             */
            NamingPhase<C> declarative(@Nonnull ComponentBuilder<WorkflowDefinition<C>> componentBuilder);

            /**
             * Auto-detects workflows on the given component.
             *
             * @return builder of customization phase.
             */
            DetectionPhase<C> autodetected(@Nonnull ComponentBuilder<Object> componentBuilder,
                                           @Nonnull Class<C> workflowContextType);
        }

        interface NamingPhase<C extends WorkflowContext> {

            /**
             * Sets workflow name.
             *
             * @param workflowName name of the workflow.
             * @return on phase.
             */
            OnPhase<C> workflowName(@Nonnull String workflowName);
        }

        interface OnPhase<C extends WorkflowContext> {

            /**
             * Specifies trigger condition for the workflow.
             *
             * @param startCondition start condition builder.
             * @return builder for workflow definition phase.
             */
            WorkflowCustomizationPhase<C> on(@Nonnull ComponentBuilder<EventCondition> startCondition);
        }

        interface WorkflowCustomizationPhase<C extends WorkflowContext> {

            /**
             * Applies customizations to workflow definition.
             *
             * @param instanceCustomization customization function.
             * @return definitions phase for the next workflow.
             */
            DetectionPhase<C> customized(
                    @Nonnull BiFunction<Configuration, WorkflowCustomization, WorkflowCustomization> instanceCustomization
            );

            /**
             * Don't apply any customizations and use defaults.
             *
             * @return definitions phase for the next workflow.
             */
            default DetectionPhase<C> notCustomized() {
                return customized((c, wc) -> wc);
            }
        }
    }
}
