/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
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
