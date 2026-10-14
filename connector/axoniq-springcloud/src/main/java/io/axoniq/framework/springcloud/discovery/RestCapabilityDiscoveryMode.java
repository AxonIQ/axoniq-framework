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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link CapabilityDiscoveryMode} that asks each {@link ServiceInstance} for its {@link MemberCapabilities} over
 * HTTP, against the endpoint served by {@link MemberCapabilitiesController}.
 * <p>
 * Because capabilities are requested again on every refresh and rarely change, responses are cached per instance and
 * revalidated with a conditional {@code If-None-Match} request. An unchanged member answers {@code 304 Not
 * Modified} with no body, so a steady-state cluster spends only the round trip. Cache entries are discarded when
 * {@link #retainOnly(Set)} reports that an instance is gone.
 * <p>
 * This application is identified by a node id generated when this mode is constructed, which
 * {@link MemberCapabilitiesController} serves along with this application's capabilities. This application's own
 * instance is asked like any other, and recognized by the node id it answers with.
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
 *     expected back. It yields {@link MemberCapabilities#INCAPABLE} under the node id the instance last answered
 *     with, keeping it in the routing ring as a member that currently handles nothing rather than removing it, so a
 *     brief outage does not reshuffle the routing of commands it would never have handled. An instance that never
 *     answered has no node id to keep, and is left out.</li>
 * </ul>
 *
 * @author Allard Buijze
 * @author Steven van Beelen
 * @since 5.4.0
 */
public class RestCapabilityDiscoveryMode implements CapabilityDiscoveryMode {

    private static final Logger logger = LoggerFactory.getLogger(RestCapabilityDiscoveryMode.class);

    /**
     * The path the capabilities endpoint is served under when none is configured.
     */
    public static final String DEFAULT_CAPABILITIES_ENDPOINT = "/axoniq-springcloud/member-capabilities";

    /**
     * How long an instance is given to answer a capabilities request when no other deadline is configured.
     * <p>
     * Kept short deliberately: capabilities are asked for on every discovery round, so this bounds how long one
     * unresponsive instance can hold up the round that rebuilds the routing ring. Applied by the Spring Boot
     * autoconfiguration to the client it contributes for capabilities requests.
     */
    public static final Duration DEFAULT_CAPABILITIES_TIMEOUT = Duration.ofSeconds(2);

    private final RestClient restClient;
    private final String capabilitiesEndpoint;

    private final String localNodeId = UUID.randomUUID().toString();
    private final AtomicReference<MemberCapabilities> localCapabilities =
            new AtomicReference<>(MemberCapabilities.INCAPABLE);
    private final Map<ServiceInstanceKey, CachedCapabilities> cache = new ConcurrentHashMap<>();
    // Kept apart from the cache, which is cleared on every failure: an instance that fails to answer keeps the node id
    // it last answered with, so that it stays the same member while it is briefly unreachable.
    private final Map<ServiceInstanceKey, String> knownNodeIds = new ConcurrentHashMap<>();

    /**
     * Constructs a {@code RestCapabilityDiscoveryMode} requesting capabilities with the given {@code restClient} from
     * the {@link #DEFAULT_CAPABILITIES_ENDPOINT}.
     *
     * @param restClient the client used to request the capabilities of other members
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
     * @param restClient           the client used to request the capabilities of other members
     * @param capabilitiesEndpoint the path, relative to a member's base URI, the capabilities endpoint is served
     *                             under
     */
    public RestCapabilityDiscoveryMode(RestClient restClient, String capabilitiesEndpoint) {
        this.restClient = Objects.requireNonNull(restClient, "The restClient must not be null.");
        this.capabilitiesEndpoint = Objects.requireNonNull(capabilitiesEndpoint,
                                                           "The capabilitiesEndpoint must not be null.");
    }

    @Override
    public String localNodeId() {
        return localNodeId;
    }

    @Override
    public void updateLocalCapabilities(MemberCapabilities capabilities) {
        this.localCapabilities.set(Objects.requireNonNull(capabilities, "The capabilities must not be null."));
    }

    @Override
    public Optional<MemberAdvertisement> discover(ServiceInstance serviceInstance) {
        Objects.requireNonNull(serviceInstance, "The serviceInstance must not be null.");
        ServiceInstanceKey key = ServiceInstanceKey.of(serviceInstance);
        Optional<MemberAdvertisement> advertisement = requestAdvertisement(serviceInstance, key);
        advertisement.ifPresentOrElse(answer -> knownNodeIds.put(key, answer.nodeId()),
                                      () -> knownNodeIds.remove(key));
        return advertisement;
    }

    private Optional<MemberAdvertisement> requestAdvertisement(ServiceInstance serviceInstance,
                                                               ServiceInstanceKey key) {
        CachedCapabilities cached = cache.get(key);
        try {
            // Inside the try because several Spring Cloud Discovery implementations throw from getUri() for an
            // instance that has not registered yet, and that is a member this round cannot reach like any other.
            URI destination = UriComponentsBuilder.fromUri(serviceInstance.getUri())
                                                  .path(capabilitiesEndpoint)
                                                  .build()
                                                  .toUri();
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
                return Optional.of(cached.advertisement());
            }
            MemberCapabilitiesPayload payload = response.getBody();
            if (payload == null) {
                logger.info("ServiceInstance [{}] answered the capabilities request with status [{}] and no body. "
                                    + "Treating it as handling nothing until the next discovery round.",
                            key, response.getStatusCode());
                cache.remove(key);
                return incapable(key);
            }
            String nodeId = payload.nodeId();
            if (nodeId == null) {
                logger.info("ServiceInstance [{}] answered the capabilities request without a node id, so it cannot "
                                    + "be told apart from other members. Leaving it out until the next discovery "
                                    + "round.", key);
                cache.remove(key);
                return Optional.empty();
            }
            MemberAdvertisement advertisement = new MemberAdvertisement(nodeId, payload.toCapabilities());
            // The tag remembered is the one the member issued, never one computed here. Echoing the member's own tag
            // is what makes the conditional request meaningful; a locally derived tag would only ever match by
            // coincidence, and would stop matching the moment a member computed its tag differently.
            String entityTag = response.getHeaders().getETag();
            if (entityTag == null) {
                // With no tag to revalidate against, this member can never answer 304, so there is nothing worth
                // remembering.
                cache.remove(key);
            } else {
                cache.put(key, new CachedCapabilities(entityTag, advertisement));
            }
            return Optional.of(advertisement);
        } catch (ServiceInstanceClientException e) {
            cache.remove(key);
            throw e;
        } catch (Exception e) {
            logger.info("Could not retrieve the capabilities of ServiceInstance [{}]. Treating it as handling "
                                + "nothing until the next discovery round.", key);
            logger.debug("ServiceInstance [{}] is reported as handling nothing due to the following exception:",
                         key, e);
            cache.remove(key);
            return incapable(key);
        }
    }

    private Optional<MemberAdvertisement> incapable(ServiceInstanceKey key) {
        return Optional.ofNullable(knownNodeIds.get(key))
                       .map(nodeId -> new MemberAdvertisement(nodeId, MemberCapabilities.INCAPABLE));
    }

    @Override
    public void retainOnly(Set<ServiceInstanceKey> knownInstances) {
        Objects.requireNonNull(knownInstances, "The knownInstances must not be null.");
        cache.keySet().retainAll(knownInstances);
        knownNodeIds.keySet().retainAll(knownInstances);
    }

    @Override
    public MemberCapabilities localCapabilities() {
        return localCapabilities.get();
    }

    private record CachedCapabilities(String entityTag, MemberAdvertisement advertisement) {

    }
}
