/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * Access to the Mongo collections the components in this module read and write, through
 * {@link org.axonframework.extensions.mongo.MongoTemplate} and its
 * {@link org.axonframework.extensions.mongo.DefaultMongoTemplate default implementation}.
 * <p>
 * These types carry the API of the Axon Framework 4 Mongo extension, to ease migration of projects that cannot move off
 * it in one go. The one departure is scope: the extension's {@code MongoTemplate} also handed out the domain event,
 * snapshot, tracking token and dead letter collections, and only the saga collection has a component here to use it.
 * <p>
 * The Mongo driver is an optional dependency of this module, so these types are only usable by a project that declares
 * it.
 */
@NullMarked
package org.axonframework.extensions.mongo;

import org.jspecify.annotations.NullMarked;
