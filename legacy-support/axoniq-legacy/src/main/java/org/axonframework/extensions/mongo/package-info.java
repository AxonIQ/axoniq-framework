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
