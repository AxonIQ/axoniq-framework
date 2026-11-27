/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.axonframework.common.AxonConfigurationException;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * Abstract implementation for MongoDB templates. A template gives access to the collections in a MongoDB database used
 * by the Axoniq Framework MongoDB components.
 *
 * @author Allard Buijze
 * @since 2.0.0
 */
public abstract class AbstractMongoTemplate {

    private final MongoDatabase database;

    /**
     * Instantiate an {@code AbstractMongoTemplate} based on the fields contained in the {@link Builder}.
     * <p>
     * Will assert that the {@link MongoDatabase} is not {@code null}, and will throw an
     * {@link AxonConfigurationException} if it is.
     *
     * @param builder the {@link Builder} used to instantiate an {@code AbstractMongoTemplate} instance
     */
    protected AbstractMongoTemplate(Builder builder) {
        builder.validate();
        this.database = builder.database;
    }

    /**
     * Returns a reference to the {@link MongoDatabase} this template operates on.
     *
     * @return the {@link MongoDatabase} this template operates on
     */
    protected MongoDatabase database() {
        return database;
    }

    /**
     * Abstract builder class to instantiate an {@link AbstractMongoTemplate}.
     * <p>
     * The {@link MongoDatabase} is a <b>hard requirement</b> and as such should be provided. It can either be provided
     * directly, or by setting a {@link MongoClient}. When choosing the latter approach, the database name can be
     * specified through {@link #mongoDatabase(MongoClient, String)}. Otherwise, the database name defaults to
     * {@code axonframework}.
     */
    public abstract static class Builder {

        private static final String DEFAULT_AXONFRAMEWORK_DATABASE = "axonframework";

        private MongoDatabase database;

        /**
         * Sets the {@link MongoDatabase} by means of providing a {@link MongoClient}. The database named
         * {@code axonframework} is retrieved from the given {@code mongoClient}.
         *
         * @param mongoClient the {@link MongoClient} to retrieve a {@link MongoDatabase} from
         * @return the current builder instance, for fluent interfacing
         */
        public Builder mongoDatabase(MongoClient mongoClient) {
            return mongoDatabase(mongoClient, DEFAULT_AXONFRAMEWORK_DATABASE);
        }

        /**
         * Sets the {@link MongoDatabase} by means of providing a {@link MongoClient}. The database named
         * {@code databaseName} is retrieved from the given {@code mongoClient}.
         *
         * @param mongoClient  the {@link MongoClient} to retrieve a {@link MongoDatabase} from
         * @param databaseName the name of the {@link MongoDatabase} to retrieve
         * @return the current builder instance, for fluent interfacing
         */
        public Builder mongoDatabase(MongoClient mongoClient, String databaseName) {
            assertNonNull(mongoClient, "MongoClient may not be null");
            this.database = mongoClient.getDatabase(databaseName);
            return this;
        }

        /**
         * Sets the {@link MongoDatabase} used by this template to connect to a MongoDB instance.
         *
         * @param mongoDatabase the {@link MongoDatabase} used by this template to connect to a MongoDB instance
         * @return the current builder instance, for fluent interfacing
         */
        public Builder mongoDatabase(MongoDatabase mongoDatabase) {
            assertNonNull(mongoDatabase, "MongoDatabase may not be null");
            this.database = mongoDatabase;
            return this;
        }

        /**
         * Validates whether the fields contained in this builder are set accordingly.
         *
         * @throws AxonConfigurationException if a field is asserted to be incorrect according to the builder's
         *                                    specifications
         */
        protected void validate() throws AxonConfigurationException {
            assertNonNull(database, "The MongoDatabase is a hard requirement and should be provided");
        }
    }
}
