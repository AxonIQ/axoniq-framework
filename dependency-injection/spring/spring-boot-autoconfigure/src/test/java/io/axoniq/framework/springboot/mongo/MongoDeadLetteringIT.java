/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.mongo;

import com.mongodb.client.model.Filters;
import io.axoniq.framework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory;
import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.mongo.deadletter.MongoSequencedDeadLetterQueueConfiguration;
import io.axoniq.framework.mongo.deadletter.MongoSequencedDeadLetterQueueFactory;
import io.axoniq.framework.mongo.token.MongoTokenStore;
import io.axoniq.framework.mongo.token.MongoTokenStoreConfiguration;
import org.axonframework.extension.spring.config.EventProcessorDefinition;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.axonframework.messaging.eventhandling.gateway.EventGateway;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.web.reactive.function.client.WebClientAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.stereotype.Component;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end test validating that, with full Spring Boot auto-configuration and a real MongoDB instance, a failing
 * event handler on a processor with {@code axon.eventhandling.processors.<name>.dlq.enabled=true} ends up as a dead
 * letter in the MongoDB dead letter collection, and that the processor's tracking token is stored in the MongoDB
 * tracking token collection.
 * <p>
 * Requires the {@code AXONIQ_LICENSE} environment variable: the licensed MongoDB addons shut down the JVM when no
 * license is present, which is why this test is skipped without it.
 *
 * @author Gerard Klijs
 */
@Testcontainers
@EnabledIfEnvironmentVariable(named = "AXONIQ_LICENSE", matches = "(?s).+")
@SpringBootTest(properties = {
        "axon.axonserver.enabled=false",
        "axon.postgresql.enabled=false",
        "axon.converter.general=jackson2",
        "axon.converter.messages=jackson2",
        "axon.converter.events=jackson2",
        // The build excludes the Spring Boot MongoDB auto-configurations for the other tests of this module.
        "spring.autoconfigure.exclude=",
        "axon.mongo.token-store.node-id=it-node",
        "axon.mongo.dead-letter-queue.max-sequences=7",
        "axon.eventhandling.processors.failing-processor.dlq.enabled=true",
        "spring.main.banner-mode=off"
})
@SpringBootConfiguration
// A MongoDB-only application: the JPA and JDBC auto-configuration of the test classpath would register their own
// TransactionManager, which, as for Axon Framework 4, takes precedence over the MongoDB one.
@EnableAutoConfiguration(exclude = {
        JmxAutoConfiguration.class,
        WebClientAutoConfiguration.class,
        DataSourceAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class
})
@EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
class MongoDeadLetteringIT {

    private static final String MONGO_IMAGE = System.getProperty("axoniq.mongo.image", "mongo:7.0");
    private static final String PROCESSOR_NAME = "failing-processor";

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer(MONGO_IMAGE).withReplicaSet();

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", () -> MONGO_CONTAINER.getReplicaSetUrl("axoniq-it"));
    }

    @Autowired
    private EventGateway eventGateway;
    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    private TokenStore tokenStore;
    @Autowired
    private SequencedDeadLetterQueueFactory queueFactory;
    @Autowired
    private MongoTokenStoreConfiguration tokenStoreConfiguration;
    @Autowired
    private MongoSequencedDeadLetterQueueConfiguration queueConfiguration;

    @Test
    void theConfigurationEnhancerRegistersTheMongoComponentsFromTheSpringBeans() {
        assertThat(tokenStore).isInstanceOf(MongoTokenStore.class);
        assertThat(queueFactory).isInstanceOf(MongoSequencedDeadLetterQueueFactory.class);
    }

    @Test
    void thePropertiesAreBoundIntoTheConfigurations() {
        assertThat(tokenStoreConfiguration.nodeId()).isEqualTo("it-node");
        assertThat(queueConfiguration.maxSequences()).isEqualTo(7);
    }

    @Test
    void failingEventHandlerEndsUpAsDeadLetterInMongoAndTokenIsStoredInMongo() {
        // given
        String aggregateId = "order-1";

        // when
        eventGateway.publish(null, new OrderPlaced(aggregateId)).orTimeout(10, TimeUnit.SECONDS).join();

        // then
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(mongoTemplate.deadLetterCollection().countDocuments()).isEqualTo(1)
        );
        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(mongoTemplate.trackingTokensCollection()
                                              .countDocuments(Filters.and(Filters.eq("processorName", PROCESSOR_NAME),
                                                                          Filters.eq("owner", "it-node"))))
                        .isPositive()
        );
    }

    /**
     * Event published by the test.
     *
     * @param orderId the identifier of the order
     */
    record OrderPlaced(String orderId) {

    }

    /**
     * Event handler that always fails, to have the event dead lettered.
     */
    @Component("failingProjection")
    static class FailingProjection {

        @EventHandler
        void on(OrderPlaced event) {
            throw new IllegalStateException("Failing on purpose for " + event.orderId());
        }
    }

    @org.springframework.context.annotation.Configuration
    static class ProcessorConfig {

        @Bean
        EventProcessorDefinition failingProcessor() {
            return EventProcessorDefinition.pooledStreaming(PROCESSOR_NAME)
                                           .assigningHandlers(descriptor -> descriptor.beanName()
                                                                                      .equals("failingProjection"))
                                           .notCustomized();
        }
    }
}
