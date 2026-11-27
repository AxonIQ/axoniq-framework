/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

/**
 * Template object providing access to the MongoDB collections used by the Axoniq Framework MongoDB components.
 * <p>
 * The {@link io.axoniq.framework.mongo.token.MongoTokenStore} uses the {@link #trackingTokensCollection()}, while the
 * dead-letter queue uses the {@link #deadLetterCollection()}.
 *
 * @since 0.7.0
 */
public interface MongoTemplate {

    /**
     * Returns a reference to the collection containing the tracking tokens.
     *
     * @return the {@link MongoCollection} containing the tracking tokens
     */
    MongoCollection<Document> trackingTokensCollection();

    /**
     * Returns a reference to the collection containing the dead letters.
     *
     * @return the {@link MongoCollection} containing the dead letters
     */
    MongoCollection<Document> deadLetterCollection();
}
