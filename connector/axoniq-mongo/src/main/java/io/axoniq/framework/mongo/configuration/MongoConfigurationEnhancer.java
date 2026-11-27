/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.configuration;

import io.axoniq.framework.mongo.DefaultMongoTemplate;
import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.mongo.token.MongoTokenStore;
import io.axoniq.framework.mongo.token.MongoTokenStoreConfiguration;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.conversion.GeneralConverter;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore;

/**
 * A {@link ConfigurationEnhancer} that is auto-loadable by the {@link ApplicationConfigurer}, making the
 * {@link MongoTokenStore} the {@link TokenStore} to use.
 * <p>
 * The enhancer only acts when a {@link MongoTemplate} component has been registered, as that is what tells it where
 * the MongoDB collections live. A {@code MongoTemplate} is therefore always required: the Spring Boot
 * auto-configuration registers one for you, whereas a declarative setup registers its own, for example a
 * {@link DefaultMongoTemplate}. The framework registers an {@link InMemoryTokenStore} as default, which this
 * enhancer replaces. A {@code TokenStore} registered explicitly, other than the default {@code InMemoryTokenStore}
 * itself, is left untouched.
 * <p>
 * The {@code MongoTokenStore} executes every operation through the
 * {@link io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider}, so it takes part in the MongoDB
 * transaction of the processing context it is invoked with. Hence, a {@code MongoTransactionalExecutorProvider}
 * component is required next to the {@code MongoTemplate}, and a
 * {@link io.axoniq.framework.mongo.transaction.ClientSessionTransactionManager} should be the registered
 * {@code TransactionManager}.
 * <p>
 * Next to the {@code TokenStore}, the {@link MongoTokenStoreConfiguration#DEFAULT default} configuration is registered
 * as a {@link MongoTokenStoreConfiguration} component, which the {@link MongoTokenStore} is constructed with. Register
 * a {@code MongoTokenStoreConfiguration} component yourself to change the settings of the token store.
 * <p>
 * Example usage:
 * <pre>{@code
 * MessagingConfigurer.create()
 *                    .componentRegistry(cr -> cr
 *                            .registerComponent(MongoTemplate.class,
 *                                               c -> DefaultMongoTemplate.builder().mongoDatabase(mongoClient).build())
 *                            .registerComponent(MongoTransactionalExecutorProvider.class,
 *                                               c -> new MongoTransactionalExecutorProvider(mongoClient))
 *                            .registerComponent(TransactionManager.class,
 *                                               c -> new ClientSessionTransactionManager(mongoClient)));
 * }</pre>
 *
 * @author Steven van Beelen
 * @since 5.5.0
 */
public class MongoConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * The {@link #order()} of this enhancer. Positioned after the Axon Server and PostgreSQL
     * {@code ConfigurationEnhancer}s ({@code Integer.MIN_VALUE + 10} and {@code Integer.MIN_VALUE + 20}).
     */
    public static final int ENHANCER_ORDER = Integer.MIN_VALUE + 30;

    @Override
    public void enhance(ComponentRegistry registry) {
        if (!registry.hasComponent(MongoTemplate.class, SearchScope.ALL)) {
            return;
        }

        registry.registerIfNotPresent(MongoTokenStoreConfiguration.class,
                                      config -> MongoTokenStoreConfiguration.DEFAULT,
                                      SearchScope.ALL);
        registry.registerIfNotPresent(TokenStore.class, MongoConfigurationEnhancer::mongoTokenStore, SearchScope.ALL);
        registry.registerDecorator(
                TokenStore.class,
                ENHANCER_ORDER,
                (config, name, delegate) -> delegate.getClass() == InMemoryTokenStore.class
                        ? mongoTokenStore(config)
                        : delegate
        );
    }

    private static MongoTokenStore mongoTokenStore(Configuration config) {
        return new MongoTokenStore(config.getComponent(MongoTemplate.class).trackingTokensCollection(),
                                   config.getComponent(MongoTransactionalExecutorProvider.class),
                                   config.getComponent(GeneralConverter.class),
                                   config.getComponent(MongoTokenStoreConfiguration.class));
    }

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }
}
