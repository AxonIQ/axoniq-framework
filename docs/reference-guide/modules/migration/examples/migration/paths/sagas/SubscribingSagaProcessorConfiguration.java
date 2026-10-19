/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package migration.paths.sagas;

import org.axonframework.extension.spring.config.SagaProcessorDefinition;
import org.axonframework.messaging.core.SubscribableEventSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class SubscribingSagaProcessorConfiguration {

    // tag::subscribing-saga-processor[]
    // Applies only when axon.eventhandling.processors[OrderSagaProcessor].mode=subscribing.
    @Bean
    SagaProcessorDefinition orderSagaEventSource(SubscribableEventSource eventSource) {
        return SagaProcessorDefinition.forSaga(OrderSaga.class)
                                      .whenSubscribing(config -> config.eventSource(eventSource));
    }
    // end::subscribing-saga-processor[]
}
