package io.axoniq.workflow.runtime;

import io.axoniq.workflow.runtime.engine.EventBasedWorkflowEngine;
import io.axoniq.workflow.runtime.engine.execution.PrettyPrintingRecordingEventStore;
import io.axoniq.workflow.runtime.engine.registry.DefaultWorkflowRepository;
import io.axoniq.workflow.runtime.engine.registry.WorkflowRepository;
import io.axoniq.workflow.runtime.engine.support.SingleEventHandlerComponent;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.FilesystemStyleComponentDescriptor;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
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
  protected EventBasedWorkflowEngine workflowEngine;
  protected DelayedPublisher delayedPublisher;
  protected WorkflowRepository<?> workflowRegistry;

  @BeforeEach
  void setUp() {

    var configurer = MessagingConfigurer.create();
    // configurer.componentRegistry(r -> r.disableEnhancer(AxonServerConfigurationEnhancer.class));
    configurer.componentRegistry(r -> r.registerEnhancer(registry ->
      registry.registerComponent(EventBasedWorkflowEngine.class, cfg ->
        new EventBasedWorkflowEngine(
          cfg.getComponent(UnitOfWorkFactory.class),
          cfg.getComponent(EventSink.class),
          cfg.getComponent(WorkflowRepository.class)
        )
      ).registerComponent(WorkflowRepository.class, cfg ->
        new DefaultWorkflowRepository()
      ).registerComponent(DelayedPublisher.class, cfg ->
        new DelayedPublisher(
          cfg.getComponent(EventSink.class),
          cfg.getComponent(EventBasedWorkflowEngine.class).getExecutor()
        )
      ).registerDecorator(EventStore.class, Integer.MAX_VALUE, (configuration, name, delegate) ->
        PrettyPrintingRecordingEventStore.eventStore(delegate)
      )
    ));
    configurer.eventProcessing(ep -> ep.pooledStreaming(ps -> ps.processor(
      EventProcessorModule
        .pooledStreaming("workflow")
        .eventHandlingComponents(req -> req.declarative(cfg -> new SingleEventHandlerComponent(
          cfg.getComponent(EventBasedWorkflowEngine.class)
        )))
        .customized((cfg, c) -> c.eventCriteria(
          set -> {
            if (set.isEmpty()) {
              return EventCriteria.havingAnyTag();
            } else {
              return EventCriteria.havingAnyTag().andBeingOneOfTypes(set);
            }
          }
        ).initialSegmentCount(1))
    )));

    configuration = configurer.start();
    workflowEngine = configuration.getComponent(EventBasedWorkflowEngine.class);
    delayedPublisher = configuration.getComponent(DelayedPublisher.class);
    workflowRegistry = configuration.getComponent(WorkflowRepository.class);

  }

  @AfterEach
  void shutdown() {
    var descriptor = new FilesystemStyleComponentDescriptor();
    configuration.getComponent(EventSink.class).describeTo(descriptor);
    workflowRegistry.describeTo(descriptor);
    logger.info(descriptor.describe());
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
