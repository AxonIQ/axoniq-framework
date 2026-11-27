/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.mongo;

import io.axoniq.framework.mongo.MongoTemplate;
import org.axonframework.common.AxonConfigurationException;
import org.junit.jupiter.api.*;
import org.springframework.data.mongodb.MongoDatabaseFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link SpringMongoTemplate}.
 *
 * @author Steven van Beelen
 */
class SpringMongoTemplateTest {

    private static final String DEFAULT_DATABASE = "template-default-db";

    private MongoDatabaseFactory factory;

    @BeforeEach
    void setUp() {
        factory = MockMongoDatabaseFactory.create(DEFAULT_DATABASE);
    }

    @Test
    void defaultCollectionNamesAreUsed() {
        // given / when
        MongoTemplate testSubject = SpringMongoTemplate.builder().factory(factory).build();

        // then
        assertThat(testSubject.trackingTokensCollection().getNamespace().getCollectionName())
                .isEqualTo("trackingtokens");
        assertThat(testSubject.deadLetterCollection().getNamespace().getCollectionName())
                .isEqualTo("deadletters");
    }

    @Test
    void customCollectionNamesAreUsed() {
        // given / when
        MongoTemplate testSubject = SpringMongoTemplate.builder()
                                                       .factory(factory)
                                                       .trackingTokensCollectionName("custom-trackingtokens")
                                                       .deadLetterCollectionName("custom-deadletters")
                                                       .build();

        // then
        assertThat(testSubject.trackingTokensCollection().getNamespace().getCollectionName())
                .isEqualTo("custom-trackingtokens");
        assertThat(testSubject.deadLetterCollection().getNamespace().getCollectionName())
                .isEqualTo("custom-deadletters");
    }

    @Test
    void defaultDatabaseOfTheFactoryIsUsedWhenNoDatabaseNameIsSet() {
        // given / when
        MongoTemplate testSubject = SpringMongoTemplate.builder().factory(factory).build();

        // then
        assertThat(testSubject.trackingTokensCollection().getNamespace().getDatabaseName())
                .isEqualTo(DEFAULT_DATABASE);
        assertThat(testSubject.deadLetterCollection().getNamespace().getDatabaseName())
                .isEqualTo(DEFAULT_DATABASE);
    }

    @Test
    void customDatabaseNameIsUsed() {
        // given / when
        MongoTemplate testSubject = SpringMongoTemplate.builder()
                                                       .factory(factory)
                                                       .databaseName("custom-databasename")
                                                       .build();

        // then
        assertThat(testSubject.trackingTokensCollection().getNamespace().getDatabaseName())
                .isEqualTo("custom-databasename");
        assertThat(testSubject.deadLetterCollection().getNamespace().getDatabaseName())
                .isEqualTo("custom-databasename");
    }

    @Test
    void factoryIsRequired() {
        // given
        SpringMongoTemplate.Builder builder = SpringMongoTemplate.builder();

        // when / then
        assertThatThrownBy(builder::build).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void emptyNamesAreRejected() {
        // given
        SpringMongoTemplate.Builder builder = SpringMongoTemplate.builder();

        // when / then
        assertThatThrownBy(() -> builder.databaseName("")).isInstanceOf(AxonConfigurationException.class);
        assertThatThrownBy(() -> builder.trackingTokensCollectionName(""))
                .isInstanceOf(AxonConfigurationException.class);
        assertThatThrownBy(() -> builder.deadLetterCollectionName(""))
                .isInstanceOf(AxonConfigurationException.class);
    }
}
