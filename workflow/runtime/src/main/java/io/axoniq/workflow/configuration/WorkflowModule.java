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

import io.axoniq.workflow.history.inmemory.WorkflowHistoryProjector;
import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.workflow.runtime.execution.WorkflowExecutionRepository;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.Module;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Workflow module encapsulates configuration for one workflow definition.
 *
 * @param <C> workflow context type.
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public interface WorkflowModule<C extends WorkflowContext> extends Module {


    /**
     * Creates a new workflow module with default settings.
     *
     * @param <C> type of the workflow context.
     * @return module builder.
     */
    static <C extends WorkflowContext> LanguagePhase.WorkflowContextFactoryPhase<C> defaults(
            @Nonnull String name, @Nonnull Class<C> contextType
    ) {
        return new SimpleWorkflowModule<>(name, contextType, true);
    }

    /**
     * Creates a new workflow module with default settings.
     *
     * @param <C> type of the workflow context.
     * @return module builder.
     */
    static <C extends WorkflowContext> ConfigurationPhase.WorkflowConfigurationRegistryPhase<C> configure(
            @Nonnull String name, @Nonnull Class<C> contextType) {
        return new SimpleWorkflowModule<>(name, contextType);
    }

    /**
     * Retrieves workflow context type.
     *
     * @return workflow context type.
     */
    Class<C> getWorkflowContextType();

    /**
     * Configuration phase for the workflow module.
     */
    interface ConfigurationPhase {

        /**
         * Phase for providing a custom workflow configuration registry.
         *
         * @param <C> type of the workflow context.
         */
        interface WorkflowConfigurationRegistryPhase<C extends WorkflowContext>
                extends WorkflowExecutionRepositoryPhase<C> {

            /**
             * Sets the workflow configuration registry.
             *
             * @param workflowConfigurationRegistry builder for the workflow configuration registry.
             * @return builder for the next phase.
             */
            WorkflowExecutionRepositoryPhase<C> workflowConfigurationRegistry(
                    @Nonnull ComponentBuilder<WorkflowConfigurationRegistry<?>> workflowConfigurationRegistry);
        }

        /**
         * Phase for providing a custom workflow execution repository.
         *
         * @param <C> type of the workflow context.
         */
        interface WorkflowExecutionRepositoryPhase<C extends WorkflowContext> extends HistoryPhase<C> {

            /**
             * Sets the workflow execution repository.
             *
             * @param workflowExecutionRepository builder for the workflow execution repository.
             * @return builder for the next phase.
             */
            HistoryPhase<C> workflowExecutionRepository(
                    @Nonnull ComponentBuilder<WorkflowExecutionRepository> workflowExecutionRepository);
        }

        /**
         * Phase for configuring whether to use history.
         *
         * @param <C> type of the workflow context.
         */
        interface HistoryPhase<C extends WorkflowContext> extends LanguagePhase.WorkflowContextFactoryPhase<C> {

            /**
             * Configures the workflow module to use history.
             *
             * @param workflowHistoryProjector builder for the workflow history projector.
             * @return builder for the next phase.
             */
            LanguagePhase.WorkflowContextFactoryPhase<C> withHistory(
                    @Nonnull ComponentBuilder<WorkflowHistoryProjector> workflowHistoryProjector);

            /**
             * Configures the workflow module to not use history.
             *
             * @return builder for the next phase.
             */
            LanguagePhase.WorkflowContextFactoryPhase<C> withoutHistory();
        }
    }

    /**
     * Defines the DSL part of the workflow definition.
     */
    interface LanguagePhase {

        /**
         * Phase for providing a custom workflow context factory.
         *
         * @param <C> type of the workflow context.
         */
        interface WorkflowContextFactoryPhase<C extends WorkflowContext> {

            /**
             * Provides a workflow context factory.
             *
             * @param workflowContextFactory factory to create a new workflow context.
             * @return builder for the state factory.
             */
            WorkflowDefinitionPhase<C> workflowContextFactory(
                    @Nonnull ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory);
        }
    }

    /**
     * Workflow definition phase.
     *
     * @param <C> type of the workflow context.
     */
    interface WorkflowDefinitionPhase<C extends WorkflowContext> {

        /**
         * Defines workflow definitions.
         *
         * @param definition definition phase.
         * @return workflow module.
         */
        WorkflowModule<C> definition(@Nonnull Function<DetectionPhase<C>, FinalizedPhase<C>> definition);


        /**
         * Phase for defining workflow detection.
         *
         * @param <C> type of the workflow context.
         */
        interface DetectionPhase<C extends WorkflowContext> {

            /**
             * Declarative workflow definition phase.
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
            FinalizedPhase<C> autodetected(@Nonnull ComponentBuilder<Object> componentBuilder);
        }

        /**
         * Phase for naming the workflow.
         *
         * @param <C> type of the workflow context.
         */
        interface NamingPhase<C extends WorkflowContext> {

            /**
             * Sets workflow name.
             *
             * @param workflowName name of the workflow.
             * @return on phase.
             */
            OnPhase<C> workflowName(@Nonnull String workflowName);
        }

        /**
         * Phase for defining workflow trigger.
         *
         * @param <C> type of the workflow context.
         */
        interface OnPhase<C extends WorkflowContext> {

            /**
             * Specifies trigger condition for the workflow.
             *
             * @param startCondition start condition builder.
             * @return builder for workflow definition phase.
             */
            WorkflowCustomizationPhase<C> on(@Nonnull ComponentBuilder<EventCondition> startCondition);
        }

        /**
         * Phase for applying workflow customizations.
         *
         * @param <C> type of the workflow context.
         */
        interface WorkflowCustomizationPhase<C extends WorkflowContext> {

            /**
             * Applies customizations to workflow definition.
             *
             * @param instanceCustomization customization function.
             * @return definitions phase for the next workflow.
             */
            FinalizedPhase<C> customized(
                    @Nonnull BiFunction<Configuration, WorkflowCustomization, WorkflowCustomization> instanceCustomization
            );

            /**
             * Don't apply any customizations and use defaults.
             *
             * @return definitions phase for the next workflow.
             */
            default FinalizedPhase<C> notCustomized() {
                return customized((c, wc) -> wc);
            }
        }

        /**
         * Terminal phase for the workflow module.
         *
         * @param <C> type of the workflow context.
         */
        interface FinalizedPhase<C> {

        }
    }
}
