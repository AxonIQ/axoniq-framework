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

package io.axoniq.framework.examples.kafka;

import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * A shipping service that publishes a deliberately shaped event to Kafka for every international shipment, and keeps
 * track of dispatched parcels by reading those events back from Kafka.
 * <p>
 * The {@code shipping} package holds the publishing side: an event handler that turns an event on the inside into an
 * event on the outside. The {@code tracking} package holds a projection that reads the outside events from Kafka. See
 * {@code application.properties} for how the sink and source are configured.
 */
@SpringBootApplication
public class ShippingApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShippingApplication.class, args);
    }

    /**
     * Keeps the progress of the event processors in memory, to keep the example self-contained; this is for the demo
     * only. A production application stores it in a database, for example with the JPA or JDBC token store, so that a
     * restart continues where it stopped. The Kafka connector's delivery guarantees also rely on that: a token store
     * that takes part in the processing context's transaction rolls the stored progress back when publishing to Kafka
     * fails, so that the events are published again.
     */
    @Bean
    public TokenStore tokenStore() {
        return new InMemoryTokenStore();
    }
}
