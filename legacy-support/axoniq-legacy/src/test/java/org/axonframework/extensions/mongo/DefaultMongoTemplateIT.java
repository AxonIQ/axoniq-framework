/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.extensions.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.axonframework.common.AxonConfigurationException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the database and saga collection configuration of {@link DefaultMongoTemplate} against a real MongoDB.
 */
@Testcontainers
class DefaultMongoTemplateIT {

    @Container
    private static final MongoDBContainer MONGO_CONTAINER = new MongoDBContainer("mongo:8.0");

    private static MongoClient mongoClient;

    @BeforeAll
    static void connect() {
        mongoClient = MongoClients.create(MONGO_CONTAINER.getConnectionString());
    }

    @AfterAll
    static void disconnect() {
        mongoClient.close();
    }

    @Nested
    class CollectionConfiguration {

        @Test
        void defaultsToTheAxonFrameworkDatabaseAndSagasCollection() {
            // given / when
            DefaultMongoTemplate template = DefaultMongoTemplate.builder().mongoDatabase(mongoClient).build();

            // then
            assertThat(template.sagaCollection().getNamespace().getDatabaseName()).isEqualTo("axonframework");
            assertThat(template.sagaCollection().getNamespace().getCollectionName()).isEqualTo("sagas");
        }

        @Test
        void usesTheConfiguredDatabaseAndSagasCollection() {
            // given / when
            DefaultMongoTemplate template = DefaultMongoTemplate.builder()
                                                                .mongoDatabase(mongoClient, "custom-database")
                                                                .build()
                                                                .withSagasCollection("custom-sagas");

            // then
            assertThat(template.sagaCollection().getNamespace().getDatabaseName()).isEqualTo("custom-database");
            assertThat(template.sagaCollection().getNamespace().getCollectionName()).isEqualTo("custom-sagas");
        }

        @Test
        void usesAProvidedMongoDatabase() {
            // given
            MongoDatabase database = mongoClient.getDatabase("provided-database");

            // when
            DefaultMongoTemplate template = DefaultMongoTemplate.builder().mongoDatabase(database).build();

            // then
            assertThat(template.sagaCollection().getNamespace().getDatabaseName()).isEqualTo("provided-database");
        }
    }

    @Nested
    class BuilderValidation {

        @Test
        void rejectsBuildingWithoutAMongoDatabase() {
            // given / when / then
            assertThatThrownBy(() -> DefaultMongoTemplate.builder().build())
                    .isInstanceOf(AxonConfigurationException.class);
        }

        @Test
        void rejectsANullSagasCollectionName() {
            // given / when / then
            assertThatThrownBy(() -> DefaultMongoTemplate.builder().sagasCollectionName(null))
                    .isInstanceOf(AxonConfigurationException.class);
        }
    }
}
