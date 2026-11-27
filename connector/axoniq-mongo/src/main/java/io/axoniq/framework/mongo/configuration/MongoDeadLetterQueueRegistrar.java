/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.configuration;

import io.axoniq.framework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory;
import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.mongo.deadletter.MongoSequencedDeadLetterQueueConfiguration;
import io.axoniq.framework.mongo.deadletter.MongoSequencedDeadLetterQueueFactory;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;

/**
 * Registers the dead-letter queue related components of the {@link MongoConfigurationEnhancer}.
 * <p>
 * Kept apart from the enhancer, as it references types of the optional {@code axoniq-dead-letter} dependency. The
 * enhancer only loads this class after having verified that dependency is on the classpath.
 *
 * @author Steven van Beelen
 * @since 5.5.0
 */
final class MongoDeadLetterQueueRegistrar {

    private MongoDeadLetterQueueRegistrar() {
        // Utility class
    }

    /**
     * Registers the default {@link MongoSequencedDeadLetterQueueConfiguration}, and a
     * {@link MongoSequencedDeadLetterQueueFactory} as the {@link SequencedDeadLetterQueueFactory} when no other is
     * present.
     *
     * @param registry the {@link ComponentRegistry} to register the factory with
     */
    static void register(ComponentRegistry registry) {
        registry.registerIfNotPresent(MongoSequencedDeadLetterQueueConfiguration.class,
                                      config -> MongoSequencedDeadLetterQueueConfiguration.DEFAULT,
                                      SearchScope.ALL);
        registry.registerIfNotPresent(
                SequencedDeadLetterQueueFactory.class,
                config -> new MongoSequencedDeadLetterQueueFactory(
                        config.getComponent(MongoTemplate.class).deadLetterCollection(),
                        config.getComponent(MongoTransactionalExecutorProvider.class),
                        config.getComponent(EventConverter.class),
                        config.getComponent(GeneralConverter.class),
                        config.getComponent(MongoSequencedDeadLetterQueueConfiguration.class)
                ),
                SearchScope.ALL
        );
    }
}
