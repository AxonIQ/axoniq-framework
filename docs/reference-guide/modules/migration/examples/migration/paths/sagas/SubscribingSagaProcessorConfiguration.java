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
