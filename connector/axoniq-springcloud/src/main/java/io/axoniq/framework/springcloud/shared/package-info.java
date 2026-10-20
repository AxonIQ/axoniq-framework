/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * The parts of the Spring Cloud connector that the command side and the query side both rely on.
 * <p>
 * {@link SpringCloudMemberDiscovery} keeps track of the members of the cluster and what each of them handles, as Spring
 * Cloud Discovery reports them, for both connectors to route with. {@link SpringCloudAxoniqAddon} identifies the
 * connector to the license entitlement system, and {@link WireCodec} is the encoding of failures both sides write to
 * each other.
 */
@NullMarked
package io.axoniq.framework.springcloud.shared;

import org.jspecify.annotations.NullMarked;
