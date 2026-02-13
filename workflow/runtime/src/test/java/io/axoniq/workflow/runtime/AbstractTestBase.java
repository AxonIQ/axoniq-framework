package io.axoniq.workflow.runtime;

import io.axoniq.workflow.dsl.simple2.MyWorkflowContext;
import io.axoniq.workflow.dsl.simple2.MyWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.WorkflowDefinitionRegistry;
import io.axoniq.workflow.runtime.engine.configuration.PrettyPrintingRecordingEventStore;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.execution.ContextToStateAdoptingStateFactory;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.InterceptingEventStore;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.eventhandling.EventSink;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public abstract class AbstractTestBase {

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
      /*
      registry.registerComponent(WorkflowEngine.class, cfg ->
        new WorkflowEngine(
          cfg.getComponent(UnitOfWorkFactory.class),
          cfg.getComponent(EventSink.class),
          cfg.getComponent(WorkflowDefinitionRegistry.class),
          cfg.getComponent(Converter.class)
        )
      ).registerComponent(WorkflowDefinitionRegistry.class, cfg ->
        new SimpleWorkflowDefinitionRegistry()
      )
       */
          registry
            .registerComponent(DelayedPublisher.class, cfg ->
              new DelayedPublisher(
                cfg.getComponent(EventSink.class),
                cfg.getComponent(WorkflowEngine.class).getExecutor()
              )
            ).registerDecorator(EventStore.class, InterceptingEventStore.DECORATION_ORDER - 1, (configuration, name, delegate) ->
              PrettyPrintingRecordingEventStore.eventStore(delegate)
            ).registerModule(
              WorkflowModule
                .declarative(MyWorkflowContext.class)
                .workflowContextFactory(c -> new MyWorkflowContextFactory(
                  c.getComponent(EventNameCustomizer.class)
                ))
                .workflowStateFactory(c -> new ContextToStateAdoptingStateFactory<>(MyWorkflowContext.class))
                .definitions(
                  getDefinitions()
                )
            )
      )
    );

    /*
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
     */

    configuration = configurer.start();
    workflowEngine = configuration.getComponent(WorkflowEngine.class);
    delayedPublisher = configuration.getComponent(DelayedPublisher.class);
    workflowRegistry = configuration.getComponent(WorkflowDefinitionRegistry.class);

  }

  protected abstract Consumer<WorkflowModule.WorkflowDefinitionPhase.DefinitionPhase<MyWorkflowContext>> getDefinitions();

  @AfterEach
  void shutdown() {
    /*
    var descriptor = new FilesystemStyleComponentDescriptor();
    configuration.getComponent(EventSink.class).describeTo(descriptor);
    workflowRegistry.describeTo(descriptor);
    logger.info(descriptor.describe());

     */
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
