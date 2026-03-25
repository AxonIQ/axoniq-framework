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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.demo.bikerental;

import io.axoniq.demo.bikerental.rental.PaymentWorkflow;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowEnhancer;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModuleEnhancer;
import io.axoniq.workflow.runtime.engine.execution.DSLAdoptingExecutionFactory;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class BikeRentalApplication {

    public static void main(String[] args) {
        SpringApplication.run(BikeRentalApplication.class, args);
    }

    /*
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                                                                    .allowIfBaseType(Object.class)
                                                                    .build();

        mapper.activateDefaultTyping(
                ptv,
                ObjectMapper.DefaultTyping.OBJECT_AND_NON_CONCRETE,
                JsonTypeInfo.As.PROPERTY
        );

        return mapper;
    }
*/
    /*
    @Bean
    public EventStorageEngine storageEngine(AxonServerConnectionManager connectionManager,
                                            EventConverter eventConverter) {
        return new AggregateBasedAxonServerEventStorageEngine(
                connectionManager.getConnection(),
                eventConverter
        );
    }

    @Bean
    public EventStore storageBasedEventStore(EventStorageEngine engine) {
        return new StorageEngineBackedEventStore(engine, new SimpleEventBus(), new AnnotationBasedTagResolver());
    }


    @Bean
    public EventSourcingConfigurer eventSourcingConfigurer(EventStorageEngine engine) {
        return EventSourcingConfigurer.create()
                                      .registerEventStorageEngine(c -> engine)
                ;
    }

     */

    @Bean
    public WorkflowModuleEnhancer paymentWorkflow() {
        return new WorkflowModuleEnhancer(
                WorkflowModule
                        .usingContext(SimpleWorkflowContext.class)
                        .workflowContextFactory(c -> new SimpleWorkflowContextFactory())
                        .workflowExecutionFactory(c -> new DSLAdoptingExecutionFactory<>(SimpleWorkflowContext.class))
                        .definitions(
                                d -> d.autodetected(
                                        c -> new PaymentWorkflow(
                                                c.getComponent(CommandGateway.class)
                                        ),
                                        SimpleWorkflowContext.class
                                )
                        )
        );
    }
    /**
     *        @Autowired
     *    public void configure(EventProcessingConfigurer config) {
     * 		config.registerPooledStreamingEventProcessor(
     * 				"io.axoniq.demo.bikerental.payment",
     * 				Configuration::eventStore,
     * 				(c, b) -> b.workerExecutor(workerExecutorService())
     * 						   .batchSize(100)
     * 		);
     *    }
     */

    /**
     *
     * @Autowired public void configure(EventProcessingConfigurer eventProcessing) {
     * eventProcessing.registerPooledStreamingEventProcessor( "PaymentSagaProcessor", Configuration::eventStore, (c, b)
     * -> b.workerExecutor(workerExecutorService()) .batchSize(100)
     * .initialToken(StreamableMessageSource::createHeadToken) ); eventProcessing.registerPooledStreamingEventProcessor(
     * "io.axoniq.demo.bikerental.rental.query", Configuration::eventStore, (c, b) ->
     * b.workerExecutor(workerExecutorService()) .batchSize(100)
     * <p>
     * ); }
     */
}
