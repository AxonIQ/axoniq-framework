package io.axoniq.workflow.runtime.engine.configuration;

import io.axoniq.workflow.runtime.api.*;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Module;

import java.util.function.Consumer;

/**
 * Workflow module encapsulates configuration for one workflow definition.
 *
 * @param <C> workflow context type.
 */
public interface WorkflowModule<C extends WorkflowContext> extends Module {

    static <C extends WorkflowContext> LanguagePhase.WorkflowContextFactoryPhase<C> declarative(Class<C> contextType) {
        return new SimpleWorkflowModule<C>(contextType.getSimpleName(), contextType);
    }

    /**
     * Retrieves workflow context type.
     *
     * @return workflow context type.
     */
    Class<C> getContextType();

    interface LanguagePhase {

        interface WorkflowContextFactoryPhase<C extends WorkflowContext> {

            WorkflowStateFactoryPhase<C> workflowContextFactory(
                    @Nonnull ComponentBuilder<WorkflowContextFactory<C>> workflowContextFactory);
        }

        interface WorkflowStateFactoryPhase<C extends WorkflowContext> {

            WorkflowDefinitionPhase<C> workflowStateFactory(
                    @Nonnull ComponentBuilder<WorkflowStateFactory> workflowStateFactory);
        }
    }

    interface WorkflowDefinitionPhase<C extends WorkflowContext> {

        WorkflowModule<C> definitions(@Nonnull Consumer<DefinitionPhase<C>> definitions);

        interface DefinitionPhase<C extends WorkflowContext> {

            OnPhase<C> declarative(@Nonnull String name);
        }

        interface OnPhase<C extends WorkflowContext> {

            NamingPhase<C> on(@Nonnull ComponentBuilder<EventCondition> startCondition);
        }

        interface NamingPhase<C extends WorkflowContext> {

            WorkflowDefinitionPhaseForWorkflow<C> workflowDefinition(
                    @Nonnull ComponentBuilder<WorkflowDefinition<C>> workflowDefinition);
        }

        interface WorkflowDefinitionPhaseForWorkflow<C extends WorkflowContext> {

            AssociationPhase<C> eventNameCustomizer(
                    @Nonnull ComponentBuilder<EventNameCustomizerProvider> eventNameCustomizerProviderComponentBuilder);
        }

        interface AssociationPhase<C extends WorkflowContext> {

            WorkflowCustomizationPhase<C> workflowIdProvider(
                    @Nonnull ComponentBuilder<AssociationProvider> workflowAssociationProvider);
        }

        interface WorkflowCustomizationPhase<C extends WorkflowContext> {

            DefinitionPhase<C> notCustomized();
        }
    }
}
