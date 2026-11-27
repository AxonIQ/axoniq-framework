/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.mongo.MongoTemplate;
import io.axoniq.framework.mongo.token.MongoTokenStore;
import io.axoniq.framework.mongo.token.MongoTokenStoreConfiguration;
import io.axoniq.framework.mongo.transaction.MongoTransactionalExecutorProvider;
import io.axoniq.framework.springboot.mongo.MockMongoDatabaseFactory;
import io.axoniq.framework.springboot.mongo.SpringMongoTemplate;
import io.axoniq.framework.springboot.mongo.SpringMongoTransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.NoTransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link MongoAxonAutoConfiguration} and the {@link MongoTransactionAutoConfiguration}
 * without a MongoDB instance. The components that the
 * {@link io.axoniq.framework.mongo.configuration.MongoConfigurationEnhancer} builds from them are validated by the
 * {@link MongoSpringBootAutoConfigurationIT}.
 *
 * @author Steven van Beelen
 */
class MongoAutoConfigurationTest {

    private static final String DEFAULT_DATABASE = "default-axoniq-db";

    private ApplicationContextRunner testContext;

    @BeforeEach
    void setUp() {
        testContext = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MongoAutoConfiguration.class))
                .withPropertyValues("axon.axonserver.enabled=false");
    }

    private ApplicationContextRunner withDatabaseFactory() {
        return testContext.withBean(MongoDatabaseFactory.class,
                                    () -> MockMongoDatabaseFactory.create(DEFAULT_DATABASE));
    }

    @Nested
    class WithMongoDatabaseFactory {

        @Test
        void registersTheSpringMongoTemplate() {
            withDatabaseFactory().run(context -> {
                assertThat(context).hasSingleBean(MongoTemplate.class);
                assertThat(context.getBean(MongoTemplate.class)).isInstanceOf(SpringMongoTemplate.class);
            });
        }

        @Test
        void registersTheTransactionComponentsInTheSameWayAsAxonFramework4() {
            withDatabaseFactory().run(context -> {
                assertThat(context).hasSingleBean(MongoTransactionalExecutorProvider.class);
                assertThat(context).hasSingleBean(MongoTransactionManager.class);
                assertThat(context).hasSingleBean(TransactionManager.class);
                assertThat(context.getBean(TransactionManager.class)).isInstanceOf(SpringMongoTransactionManager.class);
            });
        }

        @Test
        void userDeclaredMongoTransactionManagerIsWrappedByTheAxonTransactionManager() {
            MongoTransactionManager custom = new MongoTransactionManager(
                    MockMongoDatabaseFactory.create(DEFAULT_DATABASE)
            );
            withDatabaseFactory().withBean(MongoTransactionManager.class, () -> custom).run(context -> {
                assertThat(context).hasSingleBean(MongoTransactionManager.class);
                assertThat(context.getBean(MongoTransactionManager.class)).isSameAs(custom);
                assertThat(context.getBean(TransactionManager.class)).isInstanceOf(SpringMongoTransactionManager.class);
            });
        }

        @Test
        void userDeclaredTransactionManagerWins() {
            TransactionManager custom = NoTransactionManager.instance();
            withDatabaseFactory().withBean(TransactionManager.class, () -> custom).run(context -> {
                assertThat(context).hasSingleBean(TransactionManager.class);
                assertThat(context.getBean(TransactionManager.class)).isSameAs(custom);
            });
        }

        @Test
        void usesDefaultDatabaseOfTheFactoryWhenNoDatabaseNameIsConfigured() {
            withDatabaseFactory().run(context -> {
                MongoTemplate template = context.getBean(MongoTemplate.class);

                assertThat(template.trackingTokensCollection().getNamespace().getDatabaseName())
                        .isEqualTo(DEFAULT_DATABASE);
                assertThat(template.trackingTokensCollection().getNamespace().getCollectionName())
                        .isEqualTo("trackingtokens");
                assertThat(template.deadLetterCollection().getNamespace().getCollectionName())
                        .isEqualTo("deadletters");
            });
        }

        @Test
        void honoursConfiguredDatabaseName() {
            withDatabaseFactory().withPropertyValues("axon.mongo.database-name=custom-axoniq-db").run(context -> {
                MongoTemplate template = context.getBean(MongoTemplate.class);

                assertThat(template.trackingTokensCollection().getNamespace().getDatabaseName())
                        .isEqualTo("custom-axoniq-db");
                assertThat(template.deadLetterCollection().getNamespace().getDatabaseName())
                        .isEqualTo("custom-axoniq-db");
            });
        }

        @Test
        void userDeclaredMongoTemplateWins() {
            MongoDatabaseFactory factory = MockMongoDatabaseFactory.create(DEFAULT_DATABASE);
            testContext.withBean(MongoDatabaseFactory.class, () -> factory)
                       .withBean("customTemplate", MongoTemplate.class,
                                 () -> SpringMongoTemplate.builder()
                                                          .factory(factory)
                                                          .trackingTokensCollectionName("my-tokens")
                                                          .deadLetterCollectionName("my-letters")
                                                          .build())
                       .run(context -> {
                           assertThat(context).hasSingleBean(MongoTemplate.class);
                           assertThat(context).hasBean("customTemplate");
                           assertThat(context.getBean(MongoTemplate.class).trackingTokensCollection().getNamespace()
                                             .getCollectionName()).isEqualTo("my-tokens");
                       });
        }
    }

    @Nested
    class TokenStoreConfiguration {

        @Test
        void defaultsToTheDefaultConfigurationOfTheStore() {
            withDatabaseFactory().run(context -> assertThat(context.getBean(MongoTokenStoreConfiguration.class))
                    .isEqualTo(MongoTokenStoreConfiguration.DEFAULT));
        }

        @Test
        void bindsTheTokenStoreProperties() {
            withDatabaseFactory().withPropertyValues("axon.mongo.token-store.node-id=node-1",
                                                     "axon.mongo.token-store.ensure-indexes=false")
                                 .run(context -> {
                                     MongoTokenStoreConfiguration result =
                                             context.getBean(MongoTokenStoreConfiguration.class);

                                     assertThat(result.nodeId()).isEqualTo("node-1");
                                     assertThat(result.ensureIndexes()).isFalse();
                                 });
        }

        @Test
        void claimTimeoutComesFromTheSharedTokenStoreProperty() {
            withDatabaseFactory().withPropertyValues("axon.eventhandling.tokenstore.claim-timeout=42s")
                                 .run(context -> assertThat(
                                         context.getBean(MongoTokenStoreConfiguration.class).claimTimeout()
                                 ).isEqualTo(Duration.ofSeconds(42)));
        }

        @Test
        void userDeclaredConfigurationWins() {
            MongoTokenStoreConfiguration custom = MongoTokenStoreConfiguration.DEFAULT.nodeId("declared");
            withDatabaseFactory().withBean(MongoTokenStoreConfiguration.class, () -> custom)
                                 .withPropertyValues("axon.mongo.token-store.node-id=from-properties")
                                 .run(context -> assertThat(context.getBean(MongoTokenStoreConfiguration.class))
                                         .isSameAs(custom));
        }
    }

    @Nested
    class WithoutMongoDatabaseFactory {

        @Test
        void registersNothing() {
            testContext.run(context -> {
                assertThat(context).doesNotHaveBean(MongoTemplate.class);
                assertThat(context).doesNotHaveBean(MongoTokenStoreConfiguration.class);
                assertThat(context).doesNotHaveBean(MongoTransactionalExecutorProvider.class);
            });
        }

        @Test
        void backsOffWhenMongoTokenStoreClassIsAbsent() {
            withDatabaseFactory().withClassLoader(new FilteredClassLoader(MongoTokenStore.class))
                                 .run(context -> assertThat(context).doesNotHaveBean(MongoTemplate.class));
        }
    }
}
