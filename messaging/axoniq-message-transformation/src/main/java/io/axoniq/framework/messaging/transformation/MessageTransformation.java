/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.transformation;

import org.axonframework.messaging.core.Message;

/**
 * Marker interface for transformations of {@link Message messages}, grouping the kinds of message a stored
 * representation can be rewritten into when it is read.
 * <p>
 * Only {@link io.axoniq.framework.messaging.transformation.events.EventTransformation EventTransformation}
 * implements this today; command and query transformations are intended to join it, so that rolling upgrades can
 * rewrite those messages in the same way events are rewritten now.
 * <p>
 * There is deliberately no common operation here. Each kind declares the behaviour its own variants need, and the
 * chain for that kind dispatches over its sealed hierarchy, so a shared method would only erase the shape each
 * kind actually has. A generic contract can be added here if one turns out to exist.
 * <p>
 * Transformations of things that are not messages sit outside this interface. A snapshot, for instance, carries
 * entity state identified by a {@link org.axonframework.messaging.core.VersionedType VersionedType}, not a
 * message, so a snapshot transformation is not a {@code MessageTransformation}.
 *
 * @author Laura Devriendt
 * @since 5.2.0
 */
public interface MessageTransformation {

}
