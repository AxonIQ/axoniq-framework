/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.util;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.axoniq.framework.mongo.DefaultMongoTemplate;
import io.axoniq.framework.mongo.MongoTemplate;

/**
 * Utility class providing a factory for the {@link MongoTemplate} for testing.
 *
 * @author Steven van Beelen
 */
public final class MongoTemplateFactory {

    private MongoTemplateFactory() {
        // Test utility class
    }

    /**
     * Constructs a {@link MongoTemplate} connecting with the given {@code host} and {@code port}.
     *
     * @param host the host of the MongoDB instance to connect with
     * @param port the port of the MongoDB instance to connect with
     * @return a {@link MongoTemplate} connecting with the given {@code host} and {@code port}
     */
    public static MongoTemplate build(String host, int port) {
        return build(client(host, port));
    }

    /**
     * Constructs a {@link MongoTemplate} using the given {@code mongoClient}.
     *
     * @param mongoClient the {@link MongoClient} to retrieve the collections with
     * @return a {@link MongoTemplate} using the given {@code mongoClient}
     */
    public static MongoTemplate build(MongoClient mongoClient) {
        return DefaultMongoTemplate.builder()
                                   .mongoDatabase(mongoClient)
                                   .build();
    }

    /**
     * Constructs a {@link MongoClient} connecting with the given {@code host} and {@code port}.
     *
     * @param host the host of the MongoDB instance to connect with
     * @param port the port of the MongoDB instance to connect with
     * @return a {@link MongoClient} connecting with the given {@code host} and {@code port}
     */
    public static MongoClient client(String host, int port) {
        return MongoClients.create("mongodb://" + host + ":" + port);
    }
}
