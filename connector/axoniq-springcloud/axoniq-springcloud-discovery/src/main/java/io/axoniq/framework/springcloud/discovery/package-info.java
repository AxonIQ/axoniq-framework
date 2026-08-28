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
@org.jspecify.annotations.NullMarked
package io.axoniq.framework.springcloud.discovery;
