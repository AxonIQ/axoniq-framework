/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.axonframework.common.AxonConfigurationException;
import org.bson.Document;

import static org.axonframework.common.BuilderUtils.assertNonEmpty;

/**
 * {@link MongoTemplate} giving direct access to the tracking token and dead letter collections through a given
 * {@link MongoClient} or {@link MongoDatabase}. Uses a default name for each collection when none is set.
 * <p>
 * The defaults are {@code trackingtokens} for the tracking tokens and {@code deadletters} for the dead letters.
 *
 * @author Allard Buijze
 * @since 3.0.0
 */
public class DefaultMongoTemplate extends AbstractMongoTemplate implements MongoTemplate {

    private final String trackingTokensCollectionName;
    private final String deadLetterCollectionName;

    /**
     * Instantiate a {@code DefaultMongoTemplate} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link MongoDatabase} is not {@code null}, and will throw an
     * {@link AxonConfigurationException} if it is.
     *
     * @param builder the {@link Builder} used to instantiate a {@code DefaultMongoTemplate} instance
     */
    protected DefaultMongoTemplate(Builder builder) {
        super(builder);
        this.trackingTokensCollectionName = builder.trackingTokensCollectionName;
        this.deadLetterCollectionName = builder.deadLetterCollectionName;
    }

    /**
     * Instantiate a {@link Builder} to be able to create a {@code DefaultMongoTemplate}.
     * <p>
     * The {@code trackingTokensCollectionName} and {@code deadLetterCollectionName} default to {@code trackingtokens}
     * and {@code deadletters}, respectively.
     * <p>
     * The {@link MongoDatabase} is a <b>hard requirement</b> and as such should be provided. It can either be provided
     * directly, or by setting a {@link MongoClient}. When choosing the latter approach, the database name can be
     * specified through {@link Builder#mongoDatabase(MongoClient, String)}. Otherwise, the database name defaults to
     * {@code axonframework}.
     *
     * @return a {@link Builder} to be able to create a {@code DefaultMongoTemplate}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Overwrites the {@code deadLetterCollectionName} to use as the collection name for dead letters.
     *
     * @param deadLetterCollectionName the collection name for dead letters
     * @return a new {@code DefaultMongoTemplate} with the given {@code deadLetterCollectionName} set
     */
    public DefaultMongoTemplate withDeadLetterCollection(String deadLetterCollectionName) {
        return DefaultMongoTemplate.builder()
                                   .mongoDatabase(database())
                                   .trackingTokensCollectionName(trackingTokensCollectionName)
                                   .deadLetterCollectionName(deadLetterCollectionName)
                                   .build();
    }

    /**
     * Overwrites the {@code trackingTokensCollectionName} to use as the collection name for tracking tokens.
     *
     * @param trackingTokensCollectionName the collection name for tracking tokens
     * @return a new {@code DefaultMongoTemplate} with the given {@code trackingTokensCollectionName} set
     */
    public DefaultMongoTemplate withTrackingTokenCollection(String trackingTokensCollectionName) {
        return DefaultMongoTemplate.builder()
                                   .mongoDatabase(database())
                                   .trackingTokensCollectionName(trackingTokensCollectionName)
                                   .deadLetterCollectionName(deadLetterCollectionName)
                                   .build();
    }

    @Override
    public MongoCollection<Document> trackingTokensCollection() {
        return database().getCollection(trackingTokensCollectionName);
    }

    @Override
    public MongoCollection<Document> deadLetterCollection() {
        return database().getCollection(deadLetterCollectionName);
    }

    /**
     * Builder class to instantiate a {@link DefaultMongoTemplate}.
     * <p>
     * The {@code trackingTokensCollectionName} and {@code deadLetterCollectionName} default to {@code trackingtokens}
     * and {@code deadletters}, respectively.
     * <p>
     * The {@link MongoDatabase} is a <b>hard requirement</b> and as such should be provided. It can either be provided
     * directly, or by setting a {@link MongoClient}. When choosing the latter approach, the database name can be
     * specified through {@link Builder#mongoDatabase(MongoClient, String)}. Otherwise, the database name defaults to
     * {@code axonframework}.
     */
    public static class Builder extends AbstractMongoTemplate.Builder {

        private String trackingTokensCollectionName = "trackingtokens";
        private String deadLetterCollectionName = "deadletters";

        @Override
        public Builder mongoDatabase(MongoClient mongoClient) {
            super.mongoDatabase(mongoClient);
            return this;
        }

        @Override
        public Builder mongoDatabase(MongoClient mongoClient, String databaseName) {
            super.mongoDatabase(mongoClient, databaseName);
            return this;
        }

        @Override
        public Builder mongoDatabase(MongoDatabase mongoDatabase) {
            super.mongoDatabase(mongoDatabase);
            return this;
        }

        /**
         * Sets the {@code trackingTokensCollectionName} to use as the collection name for tracking tokens. Defaults to
         * {@code "trackingtokens"}.
         *
         * @param trackingTokensCollectionName the collection name for tracking tokens
         * @return the current builder instance, for fluent interfacing
         */
        public Builder trackingTokensCollectionName(String trackingTokensCollectionName) {
            assertNonEmpty(trackingTokensCollectionName, "The trackingTokensCollectionName may not be null or empty");
            this.trackingTokensCollectionName = trackingTokensCollectionName;
            return this;
        }

        /**
         * Sets the {@code deadLetterCollectionName} to use as the collection name for dead letters. Defaults to
         * {@code "deadletters"}.
         *
         * @param deadLetterCollectionName the collection name for dead letters
         * @return the current builder instance, for fluent interfacing
         */
        public Builder deadLetterCollectionName(String deadLetterCollectionName) {
            assertNonEmpty(deadLetterCollectionName, "The deadLetterCollectionName may not be null or empty");
            this.deadLetterCollectionName = deadLetterCollectionName;
            return this;
        }

        /**
         * Initializes a {@link DefaultMongoTemplate} as specified through this builder.
         *
         * @return a {@link DefaultMongoTemplate} as specified through this builder
         */
        public DefaultMongoTemplate build() {
            return new DefaultMongoTemplate(this);
        }
    }
}
