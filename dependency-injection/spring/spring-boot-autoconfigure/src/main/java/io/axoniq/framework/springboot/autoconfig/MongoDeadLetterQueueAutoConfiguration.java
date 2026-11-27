/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory;
import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.mongo.deadletter.MongoSequencedDeadLetterQueue;
import io.axoniq.framework.mongo.deadletter.MongoSequencedDeadLetterQueueConfiguration;
import io.axoniq.framework.mongo.deadletter.MongoSequencedDeadLetterQueueFactory;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import io.axoniq.framework.springboot.MongoProperties;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot autoconfiguration that registers the MongoDB-backed {@link SequencedDeadLetterQueueFactory} as a bean,
 * when {@code axoniq-mongo} and the dead-letter queue are on the classpath and a {@link MongoTemplate} bean is
 * available.
 * <p>
 * The dead-letter queue autoconfiguration of Axoniq Framework only acts when a {@code SequencedDeadLetterQueueFactory}
 * bean exists, which is why this backend registers the factory as a bean itself, in the same way as the JPA and JDBC
 * backends do, instead of leaving it to the {@link io.axoniq.framework.mongo.configuration.MongoConfigurationEnhancer}.
 * The queues it creates are configured by the {@link MongoSequencedDeadLetterQueueConfiguration} bean of
 * {@link MongoAutoConfiguration}, which the {@code axon.mongo.dead-letter-queue} properties describe.
 * <p>
 * This configuration runs after the JPA and JDBC dead-letter queue configurations, in that order of precedence, in the
 * same order as for the token store. Declare your own {@link SequencedDeadLetterQueueFactory} bean to replace it.
 *
 * @author Steven van Beelen
 * @see MongoSequencedDeadLetterQueueFactory
 * @see MongoProperties
 * @since 5.5.0
 */
@AutoConfiguration(
        afterName = {
                "org.axonframework.extension.springboot.autoconfig.ConverterAutoConfiguration",
                "io.axoniq.framework.springboot.autoconfig.MongoAutoConfiguration",
                "io.axoniq.framework.springboot.autoconfig.JpaDeadLetterQueueAutoConfiguration",
                "io.axoniq.framework.springboot.autoconfig.JdbcDeadLetterQueueAutoConfiguration"
        }
)
@ConditionalOnClass({MongoSequencedDeadLetterQueue.class, SequencedDeadLetterQueueFactory.class})
@ConditionalOnBean(MongoTemplate.class)
public class MongoDeadLetterQueueAutoConfiguration {

    /**
     * Creates a MongoDB-backed {@link SequencedDeadLetterQueueFactory} that instantiates a
     * {@link MongoSequencedDeadLetterQueue} per event handling component.
     * <p>
     * The {@code processingGroup} passed to the factory is a component-scoped identifier following the pattern
     * {@code "DeadLetterQueue[processorName][componentName]"}, used to scope dead letters in the collection.
     *
     * @param mongoTemplate      the {@link MongoTemplate} giving access to the dead letter collection
     * @param provider           the {@link MongoTransactionalExecutorProvider} providing the session that every queue
     *                           operation executes with
     * @param eventConverter     the {@link EventConverter} converting event payloads and metadata
     * @param genericConverter   the generic {@link Converter} converting diagnostics and tracking tokens
     * @param queueConfiguration the {@link MongoSequencedDeadLetterQueueConfiguration} the queues are configured with
     * @return a {@link SequencedDeadLetterQueueFactory} backed by MongoDB
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean({EventConverter.class, Converter.class})
    public SequencedDeadLetterQueueFactory mongoDeadLetterQueueFactory(
            MongoTemplate mongoTemplate,
            MongoTransactionalExecutorProvider provider,
            EventConverter eventConverter,
            Converter genericConverter,
            MongoSequencedDeadLetterQueueConfiguration queueConfiguration
    ) {
        return new MongoSequencedDeadLetterQueueFactory(
                mongoTemplate.deadLetterCollection(),
                provider,
                eventConverter,
                genericConverter,
                queueConfiguration
        );
    }
}
