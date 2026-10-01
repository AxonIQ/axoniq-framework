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
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.EventCondition;
import io.axoniq.framework.workflow.dsl.api.WorkflowContext;
import io.axoniq.framework.workflow.history.inmemory.MutableWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.WorkflowHistoryProjector;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition;
import io.axoniq.framework.workflow.runtime.execution.WorkflowConfigurationRegistry;
import io.axoniq.framework.workflow.runtime.execution.WorkflowExecutionRepository;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.Module;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorConfiguration;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A workflow {@link Module} that encapsulates the configuration for one or more workflow definitions, all sharing the
 * same {@link WorkflowContext} type defined by the generic {@code C}.
 * <p>
 * When constructed, the module registers its workflow configurations in the {@link WorkflowConfigurationRegistry},
 * which in turn is used by the {@link io.axoniq.framework.workflow.runtime.execution.WorkflowEngine WorkflowEngine} to
 * manage and execute workflows.
 *
 * <h2>Default configuration</h2>
 * Modules created through {@link #defaults(String, Class)} use sensible defaults for infrastructure components (such as
 * the {@link WorkflowConfigurationRegistry}, {@link WorkflowExecutionRepository}, and history). This is the recommended
 * entry point for most use cases. A {@link WorkflowContextFactory} has no generic default of its own, but a DSL module
 * (such as {@code axoniq-workflow-dsl} or its Kotlin counterpart) registers one automatically for its own
 * {@link WorkflowContext} type, so a custom {@link #contextType()} is typically the only case still requiring an
 * explicit {@link OptionalPhase#contextFactory(ComponentBuilder)}.
 *
 * <h2>Custom configuration</h2>
 * For advanced scenarios, {@link #configure(String, Class)} exposes the full configuration pipeline, allowing users to
 * customize history tracking before defining the workflow language.
 * <p>
 * There is one phase of the building process for a custom-configured workflow module:
 * <ul>
 *     <li>{@link HistoryPhase} - enable or disable workflow history tracking.</li>
 * </ul>
 *
 * <h2>Workflow language</h2>
 * After the (optional) configuration phase, the language phase defines one or more workflow definitions:
 * <ul>
 *     <li>{@link WorkflowDefinitionPhase} - define one or more workflow definitions, either
 *         {@link WorkflowDefinitionPhase.DetectionPhase#declarative(ComponentBuilder) declaratively} or
 *         {@link WorkflowDefinitionPhase.DetectionPhase#autodetected(ComponentBuilder) autodetected}.</li>
 * </ul>
 *
 * <h2>Terminal configuration</h2>
 * Once the workflow definitions are in place, {@link OptionalPhase} allows the {@link PooledStreamingEventProcessorConfiguration}
 * backing the module's event processor to be customized, and a custom {@link WorkflowContextFactory},
 * {@link WorkflowConfigurationRegistry}, and/or {@link WorkflowExecutionRepository} to be supplied. Reaching this
 * phase already yields a fully-formed {@link WorkflowModule}: every method here is optional, each falling back to a
 * working default when left uncalled - for {@link OptionalPhase#contextFactory(ComponentBuilder)}, that default is
 * whatever a DSL module registered for {@link #contextType()}, which only a custom {@link WorkflowContext} type
 * lacks.
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
 * @since 5.4.0
 */
public interface WorkflowModule<C extends WorkflowContext> extends Module {

    /**
     * Creates a new workflow module using default infrastructure settings, the recommended entry point for most use
     * cases.
     * <p>
     * Note that the given {@code contextType} is expected to be one originating from this project. If a custom
     * {@code contextType} is given, be sure to set the
     * {@link OptionalPhase#contextFactory(ComponentBuilder) context factory} for it as well.
     *
     * @param name        name of the workflow module
     * @param contextType the {@link WorkflowContext} type used by the workflows in this module
     * @param <C>         the type of {@link WorkflowContext} used by the workflows in this module
     * @return the {@link WorkflowDefinitionPhase} phase of this builder, for a fluent API
     */
    static <C extends WorkflowContext> WorkflowDefinitionPhase<C> defaults(
            String name,
            Class<C> contextType
    ) {
        return new SimpleWorkflowModule<>(name, contextType);
    }

    /**
     * Creates a new workflow module with a fully customizable configuration pipeline.
     * <p>
     * The returned builder starts at the {@link HistoryPhase}, allowing users to customize history tracking before
     * defining the workflow language. The {@link PooledStreamingEventProcessorConfiguration} backing the module's
     * event processor, the {@link WorkflowConfigurationRegistry}, the {@link WorkflowExecutionRepository}, and the
     * {@link WorkflowContextFactory} are all configured afterward, through the terminal {@link OptionalPhase}.
     * <p>
     * Use this entry point when you need to supply a custom implementation for history tracking.
     * <p>
     * Note that if the given {@code contextType} is <b>not</b> one originating from this project, that a custom
     * {@link OptionalPhase#contextFactory(ComponentBuilder) context factory} should be configured at all times.
     *
     * @param name        name of the workflow module
     * @param contextType the {@link WorkflowContext} type used by the workflows in this module
     * @param <C>         the type of {@link WorkflowContext} used by the workflows in this module
     * @return the {@link HistoryPhase} phase of this builder, for a fluent API
     */
    static <C extends WorkflowContext> HistoryPhase<C> configure(
            String name,
            Class<C> contextType
    ) {
        return new SimpleWorkflowModule<>(name, contextType);
    }

    /**
     * Phase of the module's building process in which workflow history tracking can be enabled or disabled.
     * <p>
     * When enabled, a {@link WorkflowHistoryProjector} collects historic information about workflow executions into the
     * configured {@link MutableWorkflowHistoryRepository}. When disabled, no history is tracked.
     * <p>
     * This phase is optional - it extends {@link WorkflowDefinitionPhase}, so it can be skipped to accept its default.
     *
     * @param <C> the type of {@link WorkflowContext} used by the workflows in this module
     */
    interface HistoryPhase<C extends WorkflowContext> extends WorkflowDefinitionPhase<C> {

        /**
         * Enables workflow history tracking, storing collected history in the given
         * {@link MutableWorkflowHistoryRepository}.
         * <p>
         * The module creates its own {@link WorkflowHistoryProjector}, injecting the repository constructed by the
         * given {@code workflowHistoryRepository}.
         *
         * @param workflowHistoryRepository a {@link ComponentBuilder} constructing the
         *                                  {@link MutableWorkflowHistoryRepository}
         * @return the {@link WorkflowDefinitionPhase} phase of this builder, for a fluent API
         */
        WorkflowDefinitionPhase<C> withHistory(
                ComponentBuilder<MutableWorkflowHistoryRepository> workflowHistoryRepository
        );

        /**
         * Disables workflow history tracking for this module.
         * <p>
         * No historic information about workflow executions will be collected.
         *
         * @return the {@link WorkflowDefinitionPhase} phase of this builder, for a fluent API
         */
        WorkflowDefinitionPhase<C> withoutHistory();

        /**
         * Applies the given {@code historyProcessorConfiguration} function to the
         * {@link PooledStreamingEventProcessorConfiguration} backing the history projector's own, dedicated event
         * processor.
         * <p>
         * The history projector runs in a separate event processor from the workflow engine's own - see
         * {@link OptionalPhase#processorConfiguration(Function)} for the engine's processor - so an application can
         * give it a different (e.g. durable) token store and tune its segment count, batch size, and claim behavior
         * independently. Meaningful only when history tracking ends up enabled; harmless to call regardless of whether
         * {@link #withHistory(ComponentBuilder)} or {@link #withoutHistory()} is (or isn't) also called, since it does
         * not itself decide whether history is enabled.
         *
         * @param historyProcessorConfiguration a function customizing the
         *                                      {@link PooledStreamingEventProcessorConfiguration} of the history
         *                                      projector's event processor
         * @return this phase, for further optional configuration or to continue to the {@link WorkflowDefinitionPhase}
         */
        HistoryPhase<C> historyProcessorConfiguration(
                Function<PooledStreamingEventProcessorConfiguration, PooledStreamingEventProcessorConfiguration> historyProcessorConfiguration
        );
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
         * The provided function receives a {@link DetectionPhase} and should return a {@link FinalizedPhase}, allowing
         * users to chain one or more workflow definitions in a fluent manner.
         *
         * @param definition a function that defines one or more workflows via the {@link DetectionPhase}
         * @return the {@link OptionalPhase} phase of this builder, for a fluent API
         */
        OptionalPhase<C> definition(Function<DetectionPhase<C>, FinalizedPhase<C>> definition);

        /**
         * Phase of the workflow definition process in which the detection strategy is chosen.
         * <p>
         * The detection strategy determines how the workflow definition and its trigger conditions are derived, either
         * automatically from annotations or manually through a declarative builder.
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

    /**
     * Terminal phase of the module's building process, in which more {@link #definition(Function) workflow definitions}
     * can be added, the {@link PooledStreamingEventProcessorConfiguration} backing the module's event processor can be
     * customized, and a custom {@link WorkflowContextFactory}, {@link WorkflowConfigurationRegistry}, and/or
     * {@link WorkflowExecutionRepository} can optionally be provided.
     * <p>
     * Reaching this phase already yields a fully-formed {@link WorkflowModule}: every method here is purely optional
     * and may be called in any order, any number of times, or not at all, with each call returning this same phase for
     * further chaining. {@link #contextFactory(ComponentBuilder)} is the one exception worth calling out - its fallback
     * is a DSL-registered default for {@link #contextType()} rather than a generic one, so a custom
     * {@link WorkflowContext} type still needs this method called, or building fails with a clear error.
     *
     * @param <C> the type of {@link WorkflowContext} used by the workflows in this module
     */
    interface OptionalPhase<C extends WorkflowContext> extends WorkflowModule<C> {

        /**
         * Add another workflow definition to this module, which already received one or more via the fluent
         * {@link WorkflowDefinitionPhase#definition(Function)} entry point.
         * <p>
         * Use this to register multiple workflow definitions of the same workflow context type into a SINGLE module so
         * they share one {@link WorkflowConfigurationRegistry} and
         * {@link io.axoniq.framework.workflow.runtime.execution.WorkflowEngine WorkflowEngine}.
         *
         * @param definition function returning a {@link WorkflowDefinitionPhase.FinalizedPhase} for an additional
         *                   workflow
         * @return this phase, for further optional configuration
         */
        OptionalPhase<C> definition(
                Function<WorkflowDefinitionPhase.DetectionPhase<C>, WorkflowDefinitionPhase.FinalizedPhase<C>> definition
        );

        /**
         * Applies the given {@code processorConfiguration} function to the
         * {@link PooledStreamingEventProcessorConfiguration} backing this module's event processor.
         * <p>
         * Standard processor settings - segment count, batch size, error handling, worker executor, and so on - live on
         * {@link PooledStreamingEventProcessorConfiguration} itself. This method is the hook through which those
         * settings are reached: the given {@link Function} is applied on top of the module's own internal customization
         * (event criteria, segment routing), so it composes with rather than replaces that wiring.
         * <p>
         * The engine component name and the event processor's own name are not configurable - the module derives both
         * from its own name, so several {@link WorkflowModule}s in one application never collide.
         *
         * @param processorConfiguration a function customizing the {@link PooledStreamingEventProcessorConfiguration}
         *                               of this module's event processor
         * @return this phase, for further optional configuration
         */
        OptionalPhase<C> processorConfiguration(
                Function<PooledStreamingEventProcessorConfiguration, PooledStreamingEventProcessorConfiguration> processorConfiguration
        );

        /**
         * Registers the given {@link ComponentBuilder} of a {@link WorkflowContextFactory} as the context factory for
         * the workflow module being built, overriding any factory a DSL module registered for {@link #contextType()}.
         * <p>
         * The factory is responsible for creating the {@link WorkflowContext} that is passed to
         * {@link WorkflowDefinition workflow definitions} during execution. Left uncalled, the module falls back to the
         * {@link WorkflowContextFactory} a DSL's own {@code ConfigurationEnhancer} registered for
         * {@link #contextType()} - for example {@code SimpleWorkflowContext} or the Kotlin DSL's context type both have
         * one registered automatically. A custom {@link WorkflowContext} type has no such enhancer, so it still
         * requires this method to be called, or building fails with a clear error.
         *
         * @param workflowContextFactory a {@link ComponentBuilder} constructing the {@link WorkflowContextFactory}
         * @return this phase, for further optional configuration
         */
        OptionalPhase<C> contextFactory(ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory);

        /**
         * Registers the given {@link ComponentBuilder} of a {@link WorkflowConfigurationRegistry} as the registry for
         * the workflow module being built.
         * <p>
         * The registry stores workflow configurations together with their corresponding
         * {@link EventCondition start conditions}, and is used by the
         * {@link io.axoniq.framework.workflow.runtime.execution.WorkflowEngine WorkflowEngine} to look up which
         * workflow to start for a given event.
         *
         * @param workflowConfigurationRegistry a {@link ComponentBuilder} constructing the
         *                                      {@link WorkflowConfigurationRegistry}
         * @return this phase, for further optional configuration
         */
        OptionalPhase<C> configurationRegistry(
                ComponentBuilder<WorkflowConfigurationRegistry<?>> workflowConfigurationRegistry
        );

        /**
         * Registers the given {@link ComponentBuilder} of a {@link WorkflowExecutionRepository} as the repository for
         * the workflow module being built.
         * <p>
         * The repository is responsible for persisting and retrieving workflow instances, where each instance is
         * identified by a unique workflow identifier and bundles configuration, context, and execution state.
         *
         * @param workflowExecutionRepository a {@link ComponentBuilder} constructing the
         *                                    {@link WorkflowExecutionRepository}
         * @return this phase, for further optional configuration
         */
        OptionalPhase<C> executionRepository(ComponentBuilder<WorkflowExecutionRepository> workflowExecutionRepository);
    }

    /**
     * Returns the {@link WorkflowContext} type used by the workflows configured in this module.
     *
     * @return the {@link WorkflowContext} type of this module
     */
    Class<C> contextType();
}
