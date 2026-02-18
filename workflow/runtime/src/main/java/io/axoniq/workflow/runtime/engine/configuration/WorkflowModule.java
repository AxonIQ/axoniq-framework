/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.api.EventCondition;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.api.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowDefinition;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.Module;

import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Workflow module encapsulates configuration for one workflow definition.
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
             * Provide workflow context factory.
             *
             * @param workflowContextFactory factory to create a new workflow context.
             * @return builder for the state factory.
             */
            WorkflowStateFactoryPhase<C> workflowContextFactory(
                    @Nonnull ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory);
        }

        interface WorkflowStateFactoryPhase<C extends WorkflowContext> {

            /**
             * Provide a workflow state factory.
             *
             * @param workflowStateFactory factory to create a new workflow state from given context.
             * @return builder for workflow definition.
             */
            WorkflowDefinitionPhase<C> workflowStateFactory(
                    @Nonnull ComponentBuilder<WorkflowStateFactory> workflowStateFactory);
        }
    }

    interface WorkflowDefinitionPhase<C extends WorkflowContext> {

        WorkflowModule<C> definitions(@Nonnull Consumer<DetectionPhase<C>> definitions);

        interface DetectionPhase<C extends WorkflowContext> {

            /**
             * Names the workflow.
             *
             * @param name workflow name.
             * @return builder for the trigger definition phase.
             */
            OnPhase<C> declarative(@Nonnull String name);

            /**
             * Auto-detects workflows on the given component.
             *
             * @return builder of customization phase.
             */
            default DetectionPhase<C> autodetected(@Nonnull Class<?> type, @Nonnull Class<C> workflowContextType) {
                var annotatedDefinitions = AutodetectedWorkflowDefinition.fromClass(type, workflowContextType);
                DetectionPhase<C> result = this;
                for (AutodetectedWorkflowDefinition<C> autodetected : annotatedDefinitions) {
                    result = this.declarative(autodetected.name())
                                 .on(autodetected.startCondition())
                                 .workflowDefinition(autodetected.workflowDefinition())
                                 .customized((c, wc) ->
                                                     wc.eventNameCustomizer(autodetected.eventNameCustomizer())
                                                       .workflowIdProvider(autodetected.workflowIdProvider().build(c))
                                 );
                }
                return result;
            }
        }

        interface OnPhase<C extends WorkflowContext> {

            /**
             * Specifies trigger condition for the workflow.
             *
             * @param startCondition start condition builder.
             * @return builder for declarative definition phase.
             */
            DeclarativeDefinitionPhase<C> on(@Nonnull ComponentBuilder<EventCondition> startCondition);
        }

        interface DeclarativeDefinitionPhase<C extends WorkflowContext> {

            /**
             * Provides workflow definition.
             *
             * @param workflowDefinition builder for workflow definition.
             * @return builder for association phase.
             */
            WorkflowCustomizationPhase<C> workflowDefinition(
                    @Nonnull ComponentBuilder<WorkflowDefinition<C>> workflowDefinition);
        }

        interface WorkflowCustomizationPhase<C extends WorkflowContext> {

            /**
             * Applies customizations to workflow definition.
             *
             * @param instanceCustomization customization function.
             * @return definitions phase for the next workflow.
             */
            DetectionPhase<C> customized(
                    @Nonnull BiFunction<Configuration, WorkflowModuleConfiguration, WorkflowModuleConfiguration> instanceCustomization
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
