/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package migration.paths.sagas;

import org.axonframework.extension.spring.config.SagaProcessorDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class SharedSagaProcessorConfiguration {

    // tag::shared-saga-processor[]
    // Both OrderSaga and ShipmentSaga carry @Namespace("orders"), so they share one processor.
    @Bean
    SagaProcessorDefinition ordersProcessor() {
        return SagaProcessorDefinition.forProcessor("orders")
                                      .whenPooledStreaming(config -> config.batchSize(50));
    }
    // end::shared-saga-processor[]
}
