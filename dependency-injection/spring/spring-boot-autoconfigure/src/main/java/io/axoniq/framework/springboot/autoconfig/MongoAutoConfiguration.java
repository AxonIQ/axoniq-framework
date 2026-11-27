/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.autoconfig;

import com.mongodb.ClientSessionOptions;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.mongo.deadletter.MongoSequencedDeadLetterQueueConfiguration;
import io.axoniq.framework.mongo.token.MongoTokenStore;
import io.axoniq.framework.mongo.token.MongoTokenStoreConfiguration;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import io.axoniq.framework.springboot.MongoProperties;
import io.axoniq.framework.springboot.mongo.SpringMongoTemplate;
import io.axoniq.framework.springboot.mongo.SpringMongoTransactionManager;
import org.axonframework.extension.springboot.TokenStoreProperties;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;

/**
 * Spring Boot autoconfiguration that adds the Spring-specific pieces to what the
 * {@link io.axoniq.framework.mongo.configuration.MongoConfigurationEnhancer} registers, when {@code axoniq-mongo} is on
 * the classpath and a {@link MongoDatabaseFactory} bean is available.
 * <p>
 * The enhancer registers the {@link MongoTokenStore} and the MongoDB-backed dead-letter queue factory from the
 * components that are available. This configuration provides those components from Spring:
 * <ul>
 *     <li>the {@link MongoTemplate}, a {@link SpringMongoTemplate} that takes part in a transaction managed by a
 *     Spring {@code MongoTransactionManager}. It uses the database named by the {@code axon.mongo.database-name}
 *     property, or the default database of the {@link MongoDatabaseFactory} when that is not set. Declare your own
 *     {@link MongoTemplate} bean to change that, for example to use other collection names;</li>
 *     <li>the {@link MongoTokenStoreConfiguration} and, when the dead-letter queue is on the classpath, the
 *     {@link MongoSequencedDeadLetterQueueConfiguration}, bound from the {@code axon.mongo.token-store} and
 *     {@code axon.mongo.dead-letter-queue} properties. Declare your own bean of either type to replace the
 *     properties;</li>
 *     <li>the transaction components, set up in the same way as it was for Axon Framework 4.</li>
 * </ul>
 * The transaction components are Spring's {@link MongoTransactionManager}, wrapped in a
 * {@link SpringMongoTransactionManager} that becomes the {@link TransactionManager}. The processing lifecycle then runs
 * in a Spring managed MongoDB transaction, which everything that obtains its database through Spring's
 * {@code MongoDatabaseUtils} joins. That includes Spring Data's {@code MongoTemplate} and repositories. The token store
 * and dead-letter queue run their operations with the session of that transaction, through the
 * {@link MongoTransactionalExecutorProvider} that this configuration registers too.
 * <p>
 * A bean of any of these types that you declare yourself takes precedence. This configuration runs after the JPA and
 * JDBC transaction configurations, so it never replaces their {@code TransactionManager}. An application that mixes
 * MongoDB with JPA or JDBC has to register the {@code TransactionalExecutorProvider} and {@code TransactionManager}
 * that fit its setup itself, as a MongoDB operation fails when no session is bound to its processing context. Note that
 * MongoDB only supports transactions on a replica set or sharded cluster.
 *
 * @author Gerard Klijs
 * @author Steven van Beelen
 * @see MongoProperties
 * @since 4.7.0
 */
@AutoConfiguration(
        afterName = {
                "org.axonframework.extension.springboot.autoconfig.JpaTransactionAutoConfiguration",
                "org.axonframework.extension.springboot.autoconfig.JdbcTransactionAutoConfiguration",
                // Spring Boot 3
                "org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration",
                // Spring Boot 4
                "org.springframework.boot.data.mongodb.autoconfigure.DataMongoAutoConfiguration"
        }
)
@ConditionalOnClass({MongoTokenStore.class, MongoTransactionalExecutorProvider.class, MongoDatabaseFactory.class})
@ConditionalOnBean(MongoDatabaseFactory.class)
@EnableConfigurationProperties({MongoProperties.class, TokenStoreProperties.class})
public class MongoAutoConfiguration {

    /**
     * Creates the {@link MongoTemplate} giving access to the collections of the Axoniq Framework MongoDB components.
     * <p>
     * To customize the collection names, declare your own {@link MongoTemplate} bean. The
     * {@code @ConditionalOnMissingBean} guard on this default will yield to it.
     *
     * @param factory    the {@link MongoDatabaseFactory} to retrieve the database from
     * @param properties the bound {@link MongoProperties}
     * @return a {@link SpringMongoTemplate} using the configured database
     */
    @Bean
    @ConditionalOnMissingBean(MongoTemplate.class)
    public MongoTemplate axonMongoTemplate(MongoDatabaseFactory factory, MongoProperties properties) {
        SpringMongoTemplate.Builder builder = SpringMongoTemplate.builder().factory(factory);
        String databaseName = properties.getDatabaseName();
        if (databaseName != null) {
            builder.databaseName(databaseName);
        }
        return builder.build();
    }

    /**
     * Creates the {@link MongoTransactionalExecutorProvider} that hands the token store and dead-letter queue the
     * MongoDB session of the processing context they are invoked with. The sessions it starts for operations outside a
     * processing context come from the {@link MongoDatabaseFactory}.
     *
     * @param databaseFactory the {@link MongoDatabaseFactory} to start sessions with
     * @return a {@link MongoTransactionalExecutorProvider} using the given {@code databaseFactory}
     */
    @Bean
    @ConditionalOnMissingBean(MongoTransactionalExecutorProvider.class)
    public MongoTransactionalExecutorProvider mongoTransactionalExecutorProvider(
            MongoDatabaseFactory databaseFactory
    ) {
        return new MongoTransactionalExecutorProvider(
                () -> databaseFactory.getSession(ClientSessionOptions.builder().build())
        );
    }

    /**
     * Creates Spring's {@link MongoTransactionManager}, writing with a majority write concern.
     * <p>
     * To change its options, declare your own {@link MongoTransactionManager} bean.
     *
     * @param databaseFactory the {@link MongoDatabaseFactory} to start transactions with
     * @return a {@link MongoTransactionManager} using the given {@code databaseFactory}
     */
    @Bean
    @ConditionalOnMissingBean
    public MongoTransactionManager mongoTransactionManager(MongoDatabaseFactory databaseFactory) {
        return new MongoTransactionManager(
                databaseFactory,
                TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build()
        );
    }

    /**
     * Creates the {@link SpringMongoTransactionManager} as the {@link TransactionManager}, which runs the processing
     * lifecycle in a transaction of the given {@code manager}, and hands its session to the processing context.
     * <p>
     * This yields to a {@code TransactionManager} bean that is registered already.
     *
     * @param manager the {@link MongoTransactionManager} to run the transactions with
     * @return a {@link SpringMongoTransactionManager} using the given {@code manager}
     */
    @Bean
    @ConditionalOnMissingBean(TransactionManager.class)
    public TransactionManager axonMongoTransactionManager(MongoTransactionManager manager) {
        return new SpringMongoTransactionManager(manager);
    }

    /**
     * Creates the {@link MongoTokenStoreConfiguration} from the {@code axon.mongo.token-store} properties. The claim
     * timeout is the one of the shared {@code axon.eventhandling.tokenstore.claim-timeout} property, as for the other
     * token stores.
     * <p>
     * To configure the token store in code, declare your own {@link MongoTokenStoreConfiguration} bean.
     *
     * @param properties           the bound {@link MongoProperties}
     * @param tokenStoreProperties the bound {@link TokenStoreProperties}, providing the claim timeout
     * @return the {@link MongoTokenStoreConfiguration} the properties describe
     */
    @Bean
    @ConditionalOnMissingBean
    public MongoTokenStoreConfiguration axonMongoTokenStoreConfiguration(MongoProperties properties,
                                                                         TokenStoreProperties tokenStoreProperties) {
        return properties.getTokenStore().toConfiguration(tokenStoreProperties.getClaimTimeout());
    }

    /**
     * Provides the configuration of the MongoDB-backed dead-letter queue, when the dead-letter queue is on the
     * classpath.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(
            name = "io.axoniq.framework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory"
    )
    static class DeadLetterQueueConfigurationProvider {

        /**
         * Creates the {@link MongoSequencedDeadLetterQueueConfiguration} from the {@code axon.mongo.dead-letter-queue}
         * properties.
         * <p>
         * To configure the queues in code, declare your own {@link MongoSequencedDeadLetterQueueConfiguration} bean.
         *
         * @param properties the bound {@link MongoProperties}
         * @return the {@link MongoSequencedDeadLetterQueueConfiguration} the properties describe
         */
        @Bean
        @ConditionalOnMissingBean
        MongoSequencedDeadLetterQueueConfiguration axonMongoDeadLetterQueueConfiguration(MongoProperties properties) {
            return properties.getDeadLetterQueue().toConfiguration();
        }
    }
}
