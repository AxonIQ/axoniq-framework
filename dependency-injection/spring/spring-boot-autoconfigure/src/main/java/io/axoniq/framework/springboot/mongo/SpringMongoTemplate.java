/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.axoniq.framework.mongo.DefaultMongoTemplate;
import io.axoniq.framework.mongo.MongoTemplate;
import org.axonframework.common.AxonConfigurationException;
import org.bson.Document;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoDatabaseUtils;

import java.util.function.Supplier;

import static org.axonframework.common.BuilderUtils.assertNonEmpty;
import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * {@link MongoTemplate} giving direct access to the tracking token and dead letter collections through a given
 * {@link MongoDatabaseFactory}. Uses a default name for each collection when none is set.
 * <p>
 * By retrieving the database through {@link MongoDatabaseUtils}, operations that do not pass a session themselves take
 * part in a transaction that a Spring {@code MongoTransactionManager} has started on the current thread. The
 * {@link SpringMongoTransactionManager} runs every processing lifecycle in such a transaction, so with it, operations
 * of this template take part in the transaction of the event handling they are invoked in.
 * <p>
 * The defaults are {@code trackingtokens} for the tracking tokens and {@code deadletters} for the dead letters.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
public class SpringMongoTemplate implements MongoTemplate {

    private final Supplier<MongoDatabase> databaseSupplier;
    private final String trackingTokensCollectionName;
    private final String deadLetterCollectionName;

    /**
     * Instantiate a {@code SpringMongoTemplate} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link MongoDatabaseFactory} is not {@code null}, and will throw an
     * {@link AxonConfigurationException} if it is.
     *
     * @param builder the {@link Builder} used to instantiate a {@code SpringMongoTemplate} instance
     */
    protected SpringMongoTemplate(Builder builder) {
        builder.validate();
        this.databaseSupplier = builder.databaseSupplier();
        this.trackingTokensCollectionName = builder.trackingTokensCollectionName;
        this.deadLetterCollectionName = builder.deadLetterCollectionName;
    }

    /**
     * Instantiate a {@link Builder} to be able to create a {@code SpringMongoTemplate}.
     * <p>
     * The {@code trackingTokensCollectionName} and {@code deadLetterCollectionName} default to {@code trackingtokens}
     * and {@code deadletters}, respectively.
     * <p>
     * The {@link MongoDatabaseFactory} is a <b>hard requirement</b> and as such should be provided.
     *
     * @return a {@link Builder} to be able to create a {@code SpringMongoTemplate}
     * @see DefaultMongoTemplate
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public MongoCollection<Document> trackingTokensCollection() {
        return databaseSupplier.get().getCollection(trackingTokensCollectionName);
    }

    @Override
    public MongoCollection<Document> deadLetterCollection() {
        return databaseSupplier.get().getCollection(deadLetterCollectionName);
    }

    /**
     * Builder class to instantiate a {@link SpringMongoTemplate}.
     * <p>
     * The {@code trackingTokensCollectionName} and {@code deadLetterCollectionName} default to {@code trackingtokens}
     * and {@code deadletters}, respectively.
     * <p>
     * The {@link MongoDatabaseFactory} is a <b>hard requirement</b> and as such should be provided.
     */
    public static class Builder {

        private MongoDatabaseFactory factory;
        private String databaseName = null;
        private String trackingTokensCollectionName = "trackingtokens";
        private String deadLetterCollectionName = "deadletters";

        /**
         * Sets the {@link MongoDatabaseFactory} used to retrieve the {@link MongoDatabase}. Needs to be set in order to
         * create a {@link SpringMongoTemplate}.
         *
         * @param factory the {@link MongoDatabaseFactory} used to create a {@link MongoDatabase}
         * @return the current builder instance, for fluent interfacing
         */
        public Builder factory(MongoDatabaseFactory factory) {
            assertNonNull(factory, "The factory may not be null");
            this.factory = factory;
            return this;
        }

        /**
         * Sets the name of the database to use. When not set, the default database of the {@link MongoDatabaseFactory}
         * is used.
         *
         * @param databaseName the name of the database to use
         * @return the current builder instance, for fluent interfacing
         */
        public Builder databaseName(String databaseName) {
            assertNonEmpty(databaseName, "The databaseName may not be empty");
            this.databaseName = databaseName;
            return this;
        }

        /**
         * Sets the collection name for tracking tokens. Defaults to {@code "trackingtokens"}.
         *
         * @param trackingTokensCollectionName the collection name for tracking tokens
         * @return the current builder instance, for fluent interfacing
         */
        public Builder trackingTokensCollectionName(String trackingTokensCollectionName) {
            assertNonEmpty(trackingTokensCollectionName, "The trackingTokensCollectionName may not be empty");
            this.trackingTokensCollectionName = trackingTokensCollectionName;
            return this;
        }

        /**
         * Sets the collection name for dead letters. Defaults to {@code "deadletters"}.
         *
         * @param deadLetterCollectionName the collection name for dead letters
         * @return the current builder instance, for fluent interfacing
         */
        public Builder deadLetterCollectionName(String deadLetterCollectionName) {
            assertNonEmpty(deadLetterCollectionName, "The deadLetterCollectionName may not be empty");
            this.deadLetterCollectionName = deadLetterCollectionName;
            return this;
        }

        /**
         * Validates whether the fields contained in this builder are set accordingly.
         *
         * @throws AxonConfigurationException if a field is asserted to be incorrect according to the builder's
         *                                    specifications
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(factory, "The MongoDatabaseFactory is a hard requirement and should be provided");
        }

        private Supplier<MongoDatabase> databaseSupplier() {
            return () -> MongoDatabaseUtils.getDatabase(databaseName, factory);
        }

        /**
         * Initializes a {@link SpringMongoTemplate} as specified through this builder.
         *
         * @return a {@link SpringMongoTemplate} as specified through this builder
         */
        public SpringMongoTemplate build() {
            return new SpringMongoTemplate(this);
        }
    }
}
