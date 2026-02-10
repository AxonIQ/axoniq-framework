package io.axoniq.workflow.runtime.taskqueue;

import io.axoniq.workflow.runtime.DelayedPublisher;
import io.axoniq.workflow.runtime.engine.impl.PrettyPrintingRecordingEventStore;
import io.axoniq.workflow.runtime.engine.impl.SingleEventHandlerComponent;
import io.axoniq.workflow.runtime.engine.impl.taskqueue.WorkflowEngine;
import io.axoniq.workflow.runtime.engine.registry.DefaultWorkflowDefinitionRegistry;
import io.axoniq.workflow.runtime.engine.registry.WorkflowDefinitionRegistry;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.FilesystemStyleComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.InterceptingEventStore;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.correlation.MessageOriginProvider;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.configuration.EventProcessorModule;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractTestBase {

  protected static final Logger logger = LoggerFactory.getLogger(AbstractTestBase.class);
  protected AxonConfiguration configuration;
  protected WorkflowEngine workflowEngine;
  protected DelayedPublisher delayedPublisher;
  protected WorkflowDefinitionRegistry<?> workflowRegistry;

  @BeforeEach
  void setUp() {

    var configurer = MessagingConfigurer.create();
    // configurer.componentRegistry(r -> r.disableEnhancer(AxonServerConfigurationEnhancer.class));
    configurer.componentRegistry(r -> r.registerEnhancer(registry ->
      registry.registerComponent(WorkflowEngine.class, cfg ->
        new WorkflowEngine(
          cfg.getComponent(UnitOfWorkFactory.class),
          cfg.getComponent(EventSink.class),
          cfg.getComponent(WorkflowDefinitionRegistry.class)
        )
      ).registerComponent(WorkflowDefinitionRegistry.class, cfg ->
        new DefaultWorkflowDefinitionRegistry()
      ).registerComponent(DelayedPublisher.class, cfg ->
        new DelayedPublisher(
          cfg.getComponent(EventSink.class),
          cfg.getComponent(WorkflowEngine.class).getExecutor()
        )
      ).registerDecorator(EventStore.class, InterceptingEventStore.DECORATION_ORDER - 1, (configuration, name, delegate) ->
        PrettyPrintingRecordingEventStore.eventStore(delegate)
      )
    ));
    configurer.eventProcessing(ep -> ep.pooledStreaming(ps -> ps.processor(
      EventProcessorModule
        .pooledStreaming("workflow")
        .eventHandlingComponents(req -> req.declarative(cfg -> new SingleEventHandlerComponent(
          cfg.getComponent(WorkflowEngine.class)
        )))
        .customized((cfg, c) -> c.eventCriteria(
            set -> {
              if (set.isEmpty()) {
                return EventCriteria.havingAnyTag();
              } else {
                return EventCriteria.havingAnyTag().andBeingOneOfTypes(set);
              }
            }
          ).initialSegmentCount(1)
        )
    )));

    configuration = configurer.start();
    workflowEngine = configuration.getComponent(WorkflowEngine.class);
    delayedPublisher = configuration.getComponent(DelayedPublisher.class);
    workflowRegistry = configuration.getComponent(WorkflowDefinitionRegistry.class);

  }

  @AfterEach
  void shutdown() {
    var descriptor = new FilesystemStyleComponentDescriptor();
    configuration.getComponent(EventSink.class).describeTo(descriptor);
    workflowRegistry.describeTo(descriptor);
    logger.info(descriptor.describe());
    workflowEngine.shutdown();
    configuration.shutdown();
  }

  static void waitWithProgress(long millis) {
    try {
      for (long i = 0; i < millis; i = i + 200) {
        Thread.sleep(i);
        logger.info("Waiting for {} / {} millis.", i, millis);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
