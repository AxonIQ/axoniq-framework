/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot;

import io.axoniq.framework.mongo.token.MongoTokenStoreConfiguration;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Properties for configuring the Axoniq Framework MongoDB components.
 * <p>
 * These properties are bound to the {@code axon.mongo} prefix.
 *
 * @author Gerard Klijs
 * @since 4.7.0
 */
@ConfigurationProperties("axon.mongo")
public class MongoProperties {

    /**
     * The name of the MongoDB database the Axoniq Framework components use. When not set, the default database of the
     * application's {@code MongoDatabaseFactory} is used.
     */
    @Nullable
    private String databaseName = null;

    /**
     * Settings for the MongoDB-backed
     * {@link org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore}.
     */
    private final TokenStore tokenStore = new TokenStore();

    /**
     * Returns the name of the MongoDB database the Axoniq Framework components use.
     *
     * @return the database name, or {@code null} if the default database of the {@code MongoDatabaseFactory} is used
     */
    @Nullable
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Sets the name of the MongoDB database the Axoniq Framework components use.
     *
     * @param databaseName the database name, or {@code null} to use the default database of the
     *                     {@code MongoDatabaseFactory}
     */
    public void setDatabaseName(@Nullable String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * Returns the settings for the MongoDB-backed token store.
     *
     * @return the token store settings
     */
    public TokenStore getTokenStore() {
        return tokenStore;
    }

    /**
     * Settings for the MongoDB-backed token store.
     */
    public static class TokenStore {

        /**
         * The identifier of this node, used to express ownership of tokens. When not set, the JVM runtime name is
         * used.
         */
        @Nullable
        private String nodeId = null;

        /**
         * Whether the token store creates the unique index on processor name and segment when it is constructed.
         * Defaults to {@code true}.
         */
        private boolean ensureIndexes = true;

        /**
         * Returns the identifier of this node, used to express ownership of tokens.
         *
         * @return the node identifier, or {@code null} if the JVM runtime name is used
         */
        @Nullable
        public String getNodeId() {
            return nodeId;
        }

        /**
         * Sets the identifier of this node, used to express ownership of tokens.
         *
         * @param nodeId the node identifier, or {@code null} to use the JVM runtime name
         */
        public void setNodeId(@Nullable String nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * Returns whether the token store creates the unique index on processor name and segment when it is
         * constructed.
         *
         * @return {@code true} if the index is created, {@code false} otherwise
         */
        public boolean isEnsureIndexes() {
            return ensureIndexes;
        }

        /**
         * Sets whether the token store creates the unique index on processor name and segment when it is constructed.
         *
         * @param ensureIndexes {@code true} to create the index, {@code false} to leave that to the developer
         */
        public void setEnsureIndexes(boolean ensureIndexes) {
            this.ensureIndexes = ensureIndexes;
        }

        /**
         * Creates a {@link MongoTokenStoreConfiguration} from these settings and the given {@code claimTimeout},
         * starting from its {@link MongoTokenStoreConfiguration#DEFAULT default}.
         *
         * @param claimTimeout the claim timeout of the store, which the shared
         *                     {@code axon.eventhandling.tokenstore.claim-timeout} property configures
         * @return the {@link MongoTokenStoreConfiguration} these settings describe
         */
        public MongoTokenStoreConfiguration toConfiguration(Duration claimTimeout) {
            MongoTokenStoreConfiguration configuration = MongoTokenStoreConfiguration.DEFAULT
                    .claimTimeout(claimTimeout)
                    .ensureIndexes(ensureIndexes);
            return nodeId != null ? configuration.nodeId(nodeId) : configuration;
        }
    }
}
