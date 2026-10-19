/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * Routing primitives for the Spring Cloud connector: the cluster {@link
 * io.axoniq.framework.springcloud.routing.Member members}, their {@link
 * io.axoniq.framework.springcloud.routing.MemberCapabilities capabilities}, and the {@link
 * io.axoniq.framework.springcloud.routing.ConsistentHash} ring that picks one for a given routing key.
 * <p>
 * These types carry no transport or discovery concerns, and depend on nothing beyond
 * {@link org.axonframework.messaging.core.QualifiedName}.
 */
@NullMarked
package io.axoniq.framework.springcloud.routing;

import org.jspecify.annotations.NullMarked;
