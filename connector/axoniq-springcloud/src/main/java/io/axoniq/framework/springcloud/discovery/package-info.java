/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * Discovery of the {@link io.axoniq.framework.springcloud.routing.MemberCapabilities capabilities} of the
 * {@code ServiceInstance}s reported by Spring Cloud Discovery.
 * <p>
 * A {@link io.axoniq.framework.springcloud.discovery.CapabilityDiscoveryMode} answers the one question the routing
 * ring needs answered per instance: which messages does it handle. {@link
 * io.axoniq.framework.springcloud.discovery.RestCapabilityDiscoveryMode} answers it over HTTP against the endpoint
 * served by {@link io.axoniq.framework.springcloud.discovery.MemberCapabilitiesController}, and {@link
 * io.axoniq.framework.springcloud.discovery.IgnoreListingDiscoveryMode} keeps instances that answer with a client
 * error out of the way for a while.
 */
@NullMarked
package io.axoniq.framework.springcloud.discovery;

import org.jspecify.annotations.NullMarked;
