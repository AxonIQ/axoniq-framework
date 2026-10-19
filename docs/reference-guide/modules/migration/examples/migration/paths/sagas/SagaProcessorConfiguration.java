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
class SagaProcessorConfiguration {

    // tag::replay-into-saga[]
    @Bean
    SagaProcessorDefinition replayIntoOrderSaga() {
        return SagaProcessorDefinition.forSaga(OrderSaga.class)
                                      .whenPooledStreaming(config -> config.initialToken(
                                              source -> source.firstToken(null)
                                      ));
    }
    // end::replay-into-saga[]

    // tag::saga-segments[]
    @Bean
    SagaProcessorDefinition orderSagaSegments() {
        return SagaProcessorDefinition.forSaga(OrderSaga.class)
                                      .whenPooledStreaming(config -> config.initialSegmentCount(4));
    }
    // end::saga-segments[]
}
