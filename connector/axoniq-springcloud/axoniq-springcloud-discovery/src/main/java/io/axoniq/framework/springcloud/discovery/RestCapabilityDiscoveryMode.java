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

import io.axoniq.framework.springcloud.routing.MemberCapabilities;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link CapabilityDiscoveryMode} that asks each {@link ServiceInstance} for its {@link MemberCapabilities} over
 * HTTP, against the endpoint served by {@link MemberCapabilitiesController}.
 * <p>
 * Because capabilities are requested on every discovery heartbeat and rarely change, responses are cached per instance
 * and revalidated with a conditional {@code If-None-Match} request. An unchanged member answers {@code 304 Not
 * Modified} with no body, so a steady-state cluster spends only the round trip. Cache entries are discarded when
 * {@link #retainOnly(Set)} reports that an instance is gone.
 * <p>
 * Selecting this mode makes {@link MemberCapabilitiesController} a requirement: every member of the cluster must serve
 * the same endpoint, or its peers cannot learn what it handles.
 * <p>
 * Failures are distinguished by kind, because they mean different things:
 * <ul>
 *     <li>A <em>client</em> error means the instance is not serving this endpoint at all — a different service, or an
 *     older version of this one. It surfaces as a {@link ServiceInstanceClientException} so that
 *     {@link IgnoreListingDiscoveryMode} can stop asking for a while.</li>
 *     <li>Any <em>other</em> failure — a refused connection, a timeout, a server error — means the instance is
 *     expected back. It yields {@link MemberCapabilities#INCAPABLE}, keeping the instance in the routing ring as a
 *     member that currently handles nothing rather than removing it, so a brief outage does not reshuffle the routing
 *     of commands it would never have handled.</li>
 * </ul>
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
public class RestCapabilityDiscoveryMode implements CapabilityDiscoveryMode {

    private static final Logger logger = LoggerFactory.getLogger(RestCapabilityDiscoveryMode.class);

    /**
     * The path the capabilities endpoint is served under when none is configured.
     */
    public static final String DEFAULT_CAPABILITIES_ENDPOINT = "/axoniq-springcloud/member-capabilities";

    private final RestClient restClient;
    private final String capabilitiesEndpoint;

    private final AtomicReference<@Nullable ServiceInstance> localInstance = new AtomicReference<>();
    private final AtomicReference<MemberCapabilities> localCapabilities =
            new AtomicReference<>(MemberCapabilities.INCAPABLE);
    private final Map<ServiceInstanceKey, CachedCapabilities> cache = new ConcurrentHashMap<>();

    /**
     * Constructs a {@code RestCapabilityDiscoveryMode} requesting capabilities with the given {@code restClient} from
     * the {@link #DEFAULT_CAPABILITIES_ENDPOINT}.
     *
     * @param restClient The client used to request the capabilities of other members.
     */
    public RestCapabilityDiscoveryMode(RestClient restClient) {
        this(restClient, DEFAULT_CAPABILITIES_ENDPOINT);
    }

    /**
     * Constructs a {@code RestCapabilityDiscoveryMode} requesting capabilities with the given {@code restClient} from
     * the given {@code capabilitiesEndpoint}.
     * <p>
     * The {@code capabilitiesEndpoint} must match the path {@link MemberCapabilitiesController} is mapped to on every
     * member of the cluster.
     *
     * @param restClient           The client used to request the capabilities of other members.
     * @param capabilitiesEndpoint The path, relative to a member's base URI, the capabilities endpoint is served
     *                             under.
     */
    public RestCapabilityDiscoveryMode(RestClient restClient, String capabilitiesEndpoint) {
        this.restClient = Objects.requireNonNull(restClient, "The restClient cannot be null.");
        this.capabilitiesEndpoint = Objects.requireNonNull(capabilitiesEndpoint,
                                                           "The capabilitiesEndpoint cannot be null.");
    }

    @Override
    public void updateLocalCapabilities(ServiceInstance localInstance, MemberCapabilities capabilities) {
        this.localInstance.set(Objects.requireNonNull(localInstance, "The localInstance cannot be null."));
        this.localCapabilities.set(Objects.requireNonNull(capabilities, "The capabilities cannot be null."));
    }

    @Override
    public Optional<MemberCapabilities> capabilities(ServiceInstance serviceInstance) {
        Objects.requireNonNull(serviceInstance, "The serviceInstance cannot be null.");
        if (isLocal(serviceInstance)) {
            return Optional.of(localCapabilities.get());
        }
        return Optional.of(requestCapabilities(serviceInstance));
    }

    private MemberCapabilities requestCapabilities(ServiceInstance serviceInstance) {
        ServiceInstanceKey key = ServiceInstanceKey.of(serviceInstance);
        CachedCapabilities cached = cache.get(key);
        URI destination = UriComponentsBuilder.fromUri(serviceInstance.getUri())
                                              .path(capabilitiesEndpoint)
                                              .build()
                                              .toUri();
        try {
            ResponseEntity<MemberCapabilitiesPayload> response =
                    restClient.get()
                              .uri(destination)
                              .headers(headers -> {
                                  if (cached != null) {
                                      headers.setIfNoneMatch(cached.entityTag());
                                  }
                              })
                              .retrieve()
                              .onStatus(HttpStatusCode::is4xxClientError, (request, clientResponse) -> {
                                  throw new ServiceInstanceClientException(
                                          ("ServiceInstance [%s] answered the capabilities request at [%s] "
                                                  + "with client error [%s].")
                                                  .formatted(key, destination, clientResponse.getStatusCode())
                                  );
                              })
                              .toEntity(MemberCapabilitiesPayload.class);

            if (response.getStatusCode() == HttpStatus.NOT_MODIFIED && cached != null) {
                return cached.capabilities();
            }
            MemberCapabilitiesPayload payload = response.getBody();
            if (payload == null) {
                logger.info("ServiceInstance [{}] answered the capabilities request with status [{}] and no body. "
                                    + "Treating it as handling nothing until the next discovery round.",
                            key, response.getStatusCode());
                cache.remove(key);
                return MemberCapabilities.INCAPABLE;
            }
            MemberCapabilities capabilities = payload.toCapabilities();
            // The tag remembered is the one the member issued, never one computed here. Echoing the member's own tag
            // is what makes the conditional request meaningful; a locally derived tag would only ever match by
            // coincidence, and would stop matching the moment a member computed its tag differently.
            String entityTag = response.getHeaders().getETag();
            if (entityTag == null) {
                // With no tag to revalidate against, this member can never answer 304, so there is nothing worth
                // remembering.
                cache.remove(key);
            } else {
                cache.put(key, new CachedCapabilities(entityTag, capabilities));
            }
            return capabilities;
        } catch (ServiceInstanceClientException e) {
            cache.remove(key);
            throw e;
        } catch (Exception e) {
            logger.info("Could not retrieve the capabilities of ServiceInstance [{}] at [{}]. Treating it as handling "
                                + "nothing until the next discovery round.", key, destination);
            logger.debug("ServiceInstance [{}] is reported as handling nothing due to the following exception:",
                         key, e);
            cache.remove(key);
            return MemberCapabilities.INCAPABLE;
        }
    }

    @Override
    public void retainOnly(Set<ServiceInstanceKey> knownInstances) {
        Objects.requireNonNull(knownInstances, "The knownInstances cannot be null.");
        cache.keySet().retainAll(knownInstances);
    }

    @Override
    public MemberCapabilities localCapabilities() {
        return localCapabilities.get();
    }

    private boolean isLocal(ServiceInstance serviceInstance) {
        ServiceInstance local = localInstance.get();
        if (local == null) {
            return false;
        }
        return Objects.equals(ServiceInstanceKey.of(serviceInstance), ServiceInstanceKey.of(local))
                || Objects.equals(serviceInstance.getUri(), local.getUri());
    }

    private record CachedCapabilities(String entityTag, MemberCapabilities capabilities) {

    }
}
