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
package io.axoniq.workflow.runtime.test;

import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowDefinitionRegistry;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.execution.ContextToStateAdoptingStateFactory;
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.FilesystemStyleComponentDescriptor;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public abstract class AbstractTestBase {
  protected final Logger logger = LoggerFactory.getLogger(getClass());
  protected AxonConfiguration configuration;
  protected WorkflowEngine workflowEngine;
  protected DelayedPublisher delayedPublisher;
  protected WorkflowDefinitionRegistry<?> workflowRegistry;


  @BeforeEach
  void setUp() {

    var configurer = MessagingConfigurer.create();

    configurer.componentRegistry(r -> r.registerEnhancer(registry ->
        registry.registerModule(
          WorkflowModule
            .declarative(SimpleWorkflowContext.class)
            .workflowContextFactory(c -> new SimpleWorkflowContextFactory(
              c.getComponent(EventNameCustomizer.class, DefaultEventNameCustomizer.Builder::eventName)
            ))
            .workflowStateFactory(c -> new ContextToStateAdoptingStateFactory<>(SimpleWorkflowContext.class))
            .definitions(
              getDefinitions()
            )
        )
      )
    );

    configuration = configurer.start();
    workflowEngine = configuration.getComponent(WorkflowEngine.class);
    workflowRegistry = configuration.getComponent(WorkflowDefinitionRegistry.class);
    delayedPublisher = configuration.getComponent(DelayedPublisher.class);

  }

  protected abstract Consumer<WorkflowModule.WorkflowDefinitionPhase.DefinitionPhase<SimpleWorkflowContext>> getDefinitions();

  @AfterEach
  void shutdown() {
    var descriptor = new FilesystemStyleComponentDescriptor();
    configuration.getComponent(EventSink.class).describeTo(descriptor);
    workflowRegistry.describeTo(descriptor);
    logger.info(descriptor.describe());
    workflowEngine.shutdown();
    configuration.shutdown();
  }

}
