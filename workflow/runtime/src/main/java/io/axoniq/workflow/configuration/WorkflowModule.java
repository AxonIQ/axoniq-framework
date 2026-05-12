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
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.Module;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A workflow {@link Module} that encapsulates the configuration for one or more workflow definitions, all sharing the
 * same {@link WorkflowContext} type defined by the generic {@code C}.
 * <p>
 * When constructed, the module registers its workflow configurations in the {@link WorkflowConfigurationRegistry},
 * which in turn is used by the
 * {@link io.axoniq.workflow.runtime.execution.WorkflowEngine WorkflowEngine} to manage and execute workflows.
 *
 * <h2>Default configuration</h2>
 * Modules created through {@link #defaults(String, Class)} use sensible defaults for infrastructure components (such as
 * the {@link WorkflowConfigurationRegistry}, {@link WorkflowExecutionRepository}, and history). This is the recommended
 * entry point for most use cases.
 *
 * <h2>Custom configuration</h2>
 * For advanced scenarios, {@link #configure(String, Class)} exposes the full configuration pipeline, allowing users to
 * supply custom implementations for infrastructure components before defining the workflow language.
 * <p>
 * There are several phases of the building process for a custom-configured workflow module:
 * <ul>
 *     <li>{@link ConfigurationPhase.WorkflowConfigurationRegistryPhase} - provide a custom
 *         {@link WorkflowConfigurationRegistry} for storing workflow configurations with their start conditions.</li>
 *     <li>{@link ConfigurationPhase.WorkflowExecutionRepositoryPhase} - provide a custom
 *         {@link WorkflowExecutionRepository} for persisting and retrieving workflow instances.</li>
 *     <li>{@link ConfigurationPhase.HistoryPhase} - enable or disable workflow history tracking.</li>
 * </ul>
 *
 * <h2>Workflow language</h2>
 * After the (optional) configuration phase, the language phase defines the workflow context and definitions:
 * <ul>
 *     <li>{@link LanguagePhase.WorkflowContextFactoryPhase} - provide the {@link WorkflowContextFactory} used to
 *         create the {@link WorkflowContext} for each workflow execution.</li>
 *     <li>{@link WorkflowDefinitionPhase} - define one or more workflow definitions, either
 *         {@link WorkflowDefinitionPhase.DetectionPhase#declarative(ComponentBuilder) declaratively} or
 *         {@link WorkflowDefinitionPhase.DetectionPhase#autodetected(ComponentBuilder) autodetected}.</li>
 * </ul>
 *
 * <h2>Autodetected workflows</h2>
 * Workflows can be built using {@link WorkflowDefinitionPhase.DetectionPhase#autodetected(ComponentBuilder)}, which
 * inspects the given component for workflow annotations and automatically derives the workflow definition.
 *
 * <h2>Declarative workflows</h2>
 * Alternatively, {@link WorkflowDefinitionPhase.DetectionPhase#declarative(ComponentBuilder)} allows manual
 * construction of the workflow through a series of builder phases: {@link WorkflowDefinitionPhase.NamingPhase naming},
 * {@link WorkflowDefinitionPhase.OnPhase trigger definition}, and
 * {@link WorkflowDefinitionPhase.WorkflowCustomizationPhase customization}.
 *
 * @param <C> the type of {@link WorkflowContext} used by this workflow {@link Module}
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public interface WorkflowModule<C extends WorkflowContext> extends Module {

    /**
     * Creates a new workflow module using default infrastructure settings.
     * <p>
     * The returned builder starts at the {@link LanguagePhase.WorkflowContextFactoryPhase}, skipping the configuration
     * phases since all infrastructure components (such as the {@link WorkflowConfigurationRegistry},
     * {@link WorkflowExecutionRepository}, and history) are configured with sensible defaults.
     * <p>
     * This is the recommended entry point for most use cases.
     *
     * @param name        name of the workflow module
     * @param contextType the {@link WorkflowContext} type used by the workflows in this module
     * @param <C>         the type of {@link WorkflowContext} used by the workflows in this module
     * @return the {@link LanguagePhase.WorkflowContextFactoryPhase} phase of this builder, for a fluent API
     */
    static <C extends WorkflowContext> LanguagePhase.WorkflowContextFactoryPhase<C> defaults(
            String name,
            Class<C> contextType
    ) {
        return new SimpleWorkflowModule<>(name, contextType, true);
    }

    /**
     * Creates a new workflow module with a fully customizable configuration pipeline.
     * <p>
     * The returned builder starts at the {@link ConfigurationPhase.WorkflowConfigurationRegistryPhase}, allowing users
     * to provide custom implementations for infrastructure components before defining the workflow language.
     * <p>
     * Use this entry point when you need to supply custom implementations for any of the infrastructure components,
     * such as the {@link WorkflowConfigurationRegistry}, {@link WorkflowExecutionRepository}, or history tracking.
     *
     * @param name        name of the workflow module
     * @param contextType the {@link WorkflowContext} type used by the workflows in this module
     * @param <C>         the type of {@link WorkflowContext} used by the workflows in this module
     * @return the {@link ConfigurationPhase.WorkflowConfigurationRegistryPhase} phase of this builder, for a fluent
     * API
     */
    static <C extends WorkflowContext> ConfigurationPhase.WorkflowConfigurationRegistryPhase<C> configure(
            String name,
            Class<C> contextType
    ) {
        return new SimpleWorkflowModule<>(name, contextType);
    }

    /**
     * Returns the {@link WorkflowContext} type used by the workflows configured in this module.
     *
     * @return the {@link WorkflowContext} type of this module
     */
    Class<C> getWorkflowContextType();

    /**
     * Defines the infrastructure configuration phases of the workflow module's building process.
     * <p>
     * These phases allow users to supply custom implementations for the infrastructure components that back workflow
     * execution.
     * <p>
     * All phases in this group are optional - each extends the next phase (and ultimately the
     * {@link LanguagePhase.WorkflowContextFactoryPhase}), so any phase can be skipped to accept its default.
     */
    interface ConfigurationPhase {

        /**
         * Phase of the module's building process in which a custom {@link WorkflowConfigurationRegistry} can be
         * provided.
         * <p>
         * The registry stores workflow configurations together with their corresponding
         * {@link EventCondition start conditions}, and is used by the
         * {@link io.axoniq.workflow.runtime.execution.WorkflowEngine WorkflowEngine} to look up which workflow to
         * start for a given event.
         *
         * @param <C> the type of {@link WorkflowContext} used by the workflows in this module
         */
        interface WorkflowConfigurationRegistryPhase<C extends WorkflowContext>
                extends WorkflowExecutionRepositoryPhase<C> {

            /**
             * Registers the given {@link ComponentBuilder} of a {@link WorkflowConfigurationRegistry} as the registry
             * for the workflow module being built.
             *
             * @param workflowConfigurationRegistry a {@link ComponentBuilder} constructing the
             *                                      {@link WorkflowConfigurationRegistry}
             * @return the {@link WorkflowExecutionRepositoryPhase} phase of this builder, for a fluent API
             */
            WorkflowExecutionRepositoryPhase<C> workflowConfigurationRegistry(
                    ComponentBuilder<WorkflowConfigurationRegistry<?>> workflowConfigurationRegistry
            );
        }

        /**
         * Phase of the module's building process in which a custom {@link WorkflowExecutionRepository} can be provided.
         * <p>
         * The repository is responsible for persisting and retrieving workflow instances, where each instance is
         * identified by a unique workflow identifier and bundles configuration, context, and execution state.
         *
         * @param <C> the type of {@link WorkflowContext} used by the workflows in this module
         */
        interface WorkflowExecutionRepositoryPhase<C extends WorkflowContext> extends HistoryPhase<C> {

            /**
             * Registers the given {@link ComponentBuilder} of a {@link WorkflowExecutionRepository} as the repository
             * for the workflow module being built.
             *
             * @param workflowExecutionRepository a {@link ComponentBuilder} constructing the
             *                                    {@link WorkflowExecutionRepository}
             * @return the {@link HistoryPhase} phase of this builder, for a fluent API
             */
            HistoryPhase<C> workflowExecutionRepository(
                    ComponentBuilder<WorkflowExecutionRepository> workflowExecutionRepository
            );
        }

        /**
         * Phase of the module's building process in which workflow history tracking can be enabled or disabled.
         * <p>
         * When enabled, a {@link WorkflowHistoryProjector} collects historic information about workflow executions.
         * When disabled, no history is tracked.
         *
         * @param <C> the type of {@link WorkflowContext} used by the workflows in this module
         */
        interface HistoryPhase<C extends WorkflowContext> extends LanguagePhase.WorkflowContextFactoryPhase<C> {

            /**
             * Enables workflow history tracking with the given {@link WorkflowHistoryProjector}.
             * <p>
             * The projector will collect historic information about workflow executions.
             *
             * @param workflowHistoryProjector a {@link ComponentBuilder} constructing the
             *                                 {@link WorkflowHistoryProjector}
             * @return the {@link LanguagePhase.WorkflowContextFactoryPhase} phase of this builder, for a fluent API
             */
            LanguagePhase.WorkflowContextFactoryPhase<C> withHistory(
                    ComponentBuilder<WorkflowHistoryProjector> workflowHistoryProjector
            );

            /**
             * Disables workflow history tracking for this module.
             * <p>
             * No historic information about workflow executions will be collected.
             *
             * @return the {@link LanguagePhase.WorkflowContextFactoryPhase} phase of this builder, for a fluent API
             */
            LanguagePhase.WorkflowContextFactoryPhase<C> withoutHistory();
        }
    }

    /**
     * Defines the workflow language phases of the module's building process.
     * <p>
     * These phases are concerned with the workflow's runtime behavior: how the {@link WorkflowContext} is created and
     * how workflow definitions are specified.
     */
    interface LanguagePhase {

        /**
         * Phase of the module's building process in which a {@link WorkflowContextFactory} should be provided.
         * <p>
         * The factory is responsible for creating the {@link WorkflowContext} that is passed to
         * {@link WorkflowDefinition workflow definitions} during execution.
         *
         * @param <C> the type of {@link WorkflowContext} used by the workflows in this module
         */
        interface WorkflowContextFactoryPhase<C extends WorkflowContext> {

            /**
             * Registers the given {@link ComponentBuilder} of a {@link WorkflowContextFactory} as the context factory
             * for the workflow module being built.
             *
             * @param workflowContextFactory a {@link ComponentBuilder} constructing the {@link WorkflowContextFactory}
             * @return the {@link WorkflowDefinitionPhase} phase of this builder, for a fluent API
             */
            WorkflowDefinitionPhase<C> workflowContextFactory(
                    ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory
            );
        }
    }

    /**
     * Phase of the module's building process in which one or more workflow definitions are provided.
     * <p>
     * Workflow definitions can be built in two ways:
     * <ul>
     *     <li>{@link DetectionPhase#autodetected(ComponentBuilder) autodetected} - inspects the given component
     *         for workflow annotations and automatically derives the workflow definition.</li>
     *     <li>{@link DetectionPhase#declarative(ComponentBuilder) declarative} - allows manual construction
     *         through a series of builder phases covering {@link NamingPhase naming},
     *         {@link OnPhase trigger conditions}, and {@link WorkflowCustomizationPhase customizations}.</li>
     * </ul>
     *
     * @param <C> the type of {@link WorkflowContext} used by the workflows in this module
     */
    interface WorkflowDefinitionPhase<C extends WorkflowContext> {

        /**
         * Defines the workflow definitions for this module.
         * <p>
         * The provided function receives a {@link DetectionPhase} and should return a {@link FinalizedPhase},
         * allowing users to chain one or more workflow definitions in a fluent manner.
         *
         * @param definition a function that defines one or more workflows via the {@link DetectionPhase}
         * @return the completed {@link WorkflowModule}
         */
        WorkflowModule<C> definition(Function<DetectionPhase<C>, FinalizedPhase<C>> definition);

        /**
         * Phase of the workflow definition process in which the detection strategy is chosen.
         * <p>
         * The detection strategy determines how the workflow definition and its trigger conditions are derived,
         * either automatically from annotations or manually through a declarative builder.
         *
         * @param <C> the type of {@link WorkflowContext} used by the workflows in this module
         */
        interface DetectionPhase<C extends WorkflowContext> {

            /**
             * Starts building a workflow definition declaratively from the given {@link WorkflowDefinition}.
             * <p>
             * This approach allows manual construction of the workflow through subsequent builder phases:
             * {@link NamingPhase naming}, {@link OnPhase trigger definition}, and
             * {@link WorkflowCustomizationPhase customization}.
             *
             * @param componentBuilder a {@link ComponentBuilder} constructing the {@link WorkflowDefinition}
             * @return the {@link NamingPhase} phase of this builder, for a fluent API
             */
            NamingPhase<C> declarative(ComponentBuilder<WorkflowDefinition<C>> componentBuilder);

            /**
             * Builds a workflow definition by auto-detecting workflow annotations on the given component.
             * <p>
             * The component is inspected for workflow annotations, from which the workflow definition, name, trigger
             * conditions, and customizations are automatically derived.
             *
             * @param componentBuilder a {@link ComponentBuilder} constructing the annotated workflow component
             * @return the {@link FinalizedPhase} of this builder, for a fluent API
             */
            FinalizedPhase<C> autodetected(ComponentBuilder<Object> componentBuilder);
        }

        /**
         * Phase of the declarative workflow definition process in which the workflow is named.
         *
         * @param <C> the type of {@link WorkflowContext} used by the workflows in this module
         */
        interface NamingPhase<C extends WorkflowContext> {

            /**
             * Sets the name for the workflow being built.
             *
             * @param workflowName the name of the workflow
             * @return the {@link OnPhase} phase of this builder, for a fluent API
             */
            OnPhase<C> workflowName(String workflowName);
        }

        /**
         * Phase of the declarative workflow definition process in which the trigger condition is defined.
         * <p>
         * The trigger condition determines which events cause a new workflow instance to be started.
         *
         * @param <C> the type of {@link WorkflowContext} used by the workflows in this module
         */
        interface OnPhase<C extends WorkflowContext> {

            /**
             * Registers the given {@link EventCondition} as the trigger for starting new instances of the workflow
             * being built.
             *
             * @param startCondition a {@link ComponentBuilder} constructing the {@link EventCondition}
             * @return the {@link WorkflowCustomizationPhase} phase of this builder, for a fluent API
             */
            WorkflowCustomizationPhase<C> on(ComponentBuilder<EventCondition> startCondition);
        }

        /**
         * Phase of the declarative workflow definition process in which optional customizations can be applied.
         * <p>
         * Customizations allow fine-tuning workflow behavior through a {@link WorkflowCustomization} instance.
         *
         * @param <C> the type of {@link WorkflowContext} used by the workflows in this module
         */
        interface WorkflowCustomizationPhase<C extends WorkflowContext> {

            /**
             * Applies the given customizations to the workflow being built.
             *
             * @param instanceCustomization a function receiving the {@link Configuration} and a
             *                              {@link WorkflowCustomization} to modify
             * @return the {@link FinalizedPhase} of this builder, for a fluent API
             */
            FinalizedPhase<C> customized(
                    BiFunction<Configuration, WorkflowCustomization, WorkflowCustomization> instanceCustomization
            );

            /**
             * Skips customization and uses default settings for the workflow being built.
             *
             * @return the {@link FinalizedPhase} of this builder, for a fluent API
             */
            default FinalizedPhase<C> notCustomized() {
                return customized((c, wc) -> wc);
            }
        }

        /**
         * Terminal phase of the workflow definition process, indicating the workflow definition is complete.
         *
         * @param <C> the type of {@link WorkflowContext} used by the workflows in this module
         */
        interface FinalizedPhase<C> {

        }
    }
}
