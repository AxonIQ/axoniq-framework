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

package io.axoniq.framework.springcloud.discovery;

import org.jspecify.annotations.Nullable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

/**
 * Serves this application's own {@link io.axoniq.framework.springcloud.routing.MemberCapabilities capabilities} over
 * HTTP, so that other members of the cluster can learn which commands this one handles.
 * <p>
 * Every member of a cluster using {@link RestCapabilityDiscoveryMode} must expose this endpoint, since that is the
 * only way capabilities travel: they cannot be published through
 * {@link org.springframework.cloud.client.ServiceInstance#getMetadata() service instance metadata}, which is fixed at
 * registration time.
 * <p>
 * The response carries an {@code ETag} derived from the capabilities themselves. A member polling this endpoint on
 * every discovery heartbeat sends its last-seen tag as {@code If-None-Match}, and an unchanged member is answered with
 * {@code 304 Not Modified} and no body. Steady-state polling therefore costs a round trip and nothing more.
 * <p>
 * This controller is registered by the Spring Boot autoconfiguration; the path it is mapped to is the value of the
 * {@code axon.springcloud.capabilities-endpoint} property, defaulting to
 * {@link RestCapabilityDiscoveryMode#DEFAULT_CAPABILITIES_ENDPOINT}. Every member of the cluster must agree on it.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
@RestController
@RequestMapping("${axon.springcloud.capabilities-endpoint:"
        + RestCapabilityDiscoveryMode.DEFAULT_CAPABILITIES_ENDPOINT + "}")
public class MemberCapabilitiesController {

    private final CapabilityDiscoveryMode discoveryMode;

    /**
     * Constructs a {@code MemberCapabilitiesController} serving the capabilities published to the given
     * {@code discoveryMode}.
     *
     * @param discoveryMode the mode holding this application's own capabilities
     */
    public MemberCapabilitiesController(CapabilityDiscoveryMode discoveryMode) {
        this.discoveryMode = Objects.requireNonNull(discoveryMode, "The discoveryMode must not be null.");
    }

    /**
     * Returns this application's own capabilities, or {@code 304 Not Modified} when the given {@code ifNoneMatch}
     * already identifies them.
     *
     * @param ifNoneMatch the {@code If-None-Match} header of the request, carrying the entity tag the requesting
     *                    member last saw, or {@code null} when it has not seen these capabilities before.
     * @return this application's capabilities with their entity tag, or an empty {@code 304 Not Modified} response
     */
    @GetMapping
    public ResponseEntity<MemberCapabilitiesPayload> localMemberCapabilities(
            @RequestHeader(value = HttpHeaders.IF_NONE_MATCH, required = false) @Nullable String ifNoneMatch
    ) {
        MemberCapabilitiesPayload payload = MemberCapabilitiesPayload.from(discoveryMode.localCapabilities());
        String entityTag = payload.quotedEntityTag();
        if (entityTag.equals(ifNoneMatch)) {
            return ResponseEntity.status(HttpStatus.NOT_MODIFIED).eTag(entityTag).build();
        }
        return ResponseEntity.ok().eTag(entityTag).body(payload);
    }
}
