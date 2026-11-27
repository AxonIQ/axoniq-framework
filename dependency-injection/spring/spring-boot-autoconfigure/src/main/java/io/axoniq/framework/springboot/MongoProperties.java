/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

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
}
