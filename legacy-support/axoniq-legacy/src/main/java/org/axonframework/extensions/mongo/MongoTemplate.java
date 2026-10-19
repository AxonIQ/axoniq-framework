/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.extensions.mongo;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

/**
 * Template object providing access to the collections necessary for the Mongo based components.
 *
 * @author Allard Buijze
 * @since 2.0
 */
public interface MongoTemplate {

    /**
     * Returns a reference to the collection containing the saga instances.
     *
     * @return MongoCollection containing the sagas
     */
    MongoCollection<Document> sagaCollection();
}
