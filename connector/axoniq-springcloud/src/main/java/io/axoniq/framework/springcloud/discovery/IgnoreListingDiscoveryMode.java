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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link CapabilityDiscoveryMode} that delegates to another, and stops asking an instance for its
 * {@link MemberCapabilities} for a while once that instance answers with a client error.
 * <p>
 * Spring Cloud Discovery reports every service registered with it, not only the ones running this connector. Asking an
 * unrelated service for its capabilities fails with a client error on every heartbeat for as long as both are
 * deployed — a steady stream of pointless requests and log noise. This mode remembers such instances and skips them
 * until the configured {@code ignorePeriod} has passed, after which they are tried once more. That expiry matters:
 * an instance may have answered with a client error because it was still starting up, and a permanent ignore-list
 * would keep it out of the cluster for good.
 * <p>
 * An ignored instance is reported as {@link Optional#empty()}, which leaves it out of the routing ring entirely. That
 * is deliberately different from how the delegate reports an instance it merely could not reach, which stays in the
 * ring as a member handling nothing.
 *
 * @author Allard Buijze
 * @author Steven van Beelen
 * @since 5.4.0
 */
public class IgnoreListingDiscoveryMode implements CapabilityDiscoveryMode {

    private static final Logger logger = LoggerFactory.getLogger(IgnoreListingDiscoveryMode.class);

    /**
     * The period an instance is ignored for when no other period is configured.
     */
    public static final Duration DEFAULT_IGNORE_PERIOD = Duration.ofMinutes(1);

    private final CapabilityDiscoveryMode delegate;
    private final Duration ignorePeriod;
    private final Clock clock;

    private final Map<ServiceInstanceKey, Instant> ignoredUntil = new ConcurrentHashMap<>();

    /**
     * Constructs an {@code IgnoreListingDiscoveryMode} around the given {@code delegate}, ignoring instances that
     * answer with a client error for {@link #DEFAULT_IGNORE_PERIOD}.
     *
     * @param delegate the mode to delegate capability discovery to
     */
    public IgnoreListingDiscoveryMode(CapabilityDiscoveryMode delegate) {
        this(delegate, DEFAULT_IGNORE_PERIOD, Clock.systemUTC());
    }

    /**
     * Constructs an {@code IgnoreListingDiscoveryMode} around the given {@code delegate}, ignoring instances that
     * answer with a client error for the given {@code ignorePeriod}.
     *
     * @param delegate     the mode to delegate capability discovery to
     * @param ignorePeriod the period an instance is ignored for after answering with a client error. Must be strictly
     *                     positive.
     */
    public IgnoreListingDiscoveryMode(CapabilityDiscoveryMode delegate, Duration ignorePeriod) {
        this(delegate, ignorePeriod, Clock.systemUTC());
    }

    /**
     * Constructs an {@code IgnoreListingDiscoveryMode} around the given {@code delegate}, ignoring instances that
     * answer with a client error for the given {@code ignorePeriod} as measured by the given {@code clock}.
     *
     * @param delegate     the mode to delegate capability discovery to
     * @param ignorePeriod the period an instance is ignored for after answering with a client error. Must be strictly
     *                     positive.
     * @param clock        the clock measuring when an ignored instance may be tried again
     */
    public IgnoreListingDiscoveryMode(CapabilityDiscoveryMode delegate, Duration ignorePeriod, Clock clock) {
        Objects.requireNonNull(ignorePeriod, "The ignorePeriod must not be null.");
        if (ignorePeriod.isNegative() || ignorePeriod.isZero()) {
            throw new IllegalArgumentException(
                    "The ignorePeriod must be strictly positive, but was [" + ignorePeriod + "]."
            );
        }
        this.delegate = Objects.requireNonNull(delegate, "The delegate must not be null.");
        this.ignorePeriod = ignorePeriod;
        this.clock = Objects.requireNonNull(clock, "The clock must not be null.");
    }

    @Override
    public void updateLocalCapabilities(ServiceInstance localInstance, MemberCapabilities capabilities) {
        Objects.requireNonNull(localInstance, "The localInstance must not be null.");
        Objects.requireNonNull(capabilities, "The capabilities must not be null.");
        delegate.updateLocalCapabilities(localInstance, capabilities);
    }

    @Override
    public MemberCapabilities localCapabilities() {
        return delegate.localCapabilities();
    }

    @Override
    public Optional<MemberCapabilities> capabilities(ServiceInstance serviceInstance) {
        Objects.requireNonNull(serviceInstance, "The serviceInstance must not be null.");
        ServiceInstanceKey key = ServiceInstanceKey.of(serviceInstance);
        if (isIgnored(key)) {
            return Optional.empty();
        }
        try {
            return delegate.capabilities(serviceInstance);
        } catch (ServiceInstanceClientException e) {
            ignoredUntil.put(key, clock.instant().plus(ignorePeriod));
            logger.info("Ignoring ServiceInstance [{}] for [{}], as it answered the capabilities request with a "
                                + "client error. It is not serving this connector's capabilities endpoint.",
                        key, ignorePeriod, e);
            return Optional.empty();
        }
    }

    private boolean isIgnored(ServiceInstanceKey key) {
        // Evaluated per entry rather than by sweeping the map, so that a discovery round over n instances stays
        // linear in n: it is called once per instance, and a sweep would make each call linear in its own right.
        Instant expiry = ignoredUntil.get(key);
        if (expiry == null) {
            return false;
        }
        if (expiry.isAfter(clock.instant())) {
            return true;
        }
        // The threshold passed, so the instance is retried in this very round rather than waiting for the next.
        ignoredUntil.remove(key, expiry);
        return false;
    }

    @Override
    public void retainOnly(Set<ServiceInstanceKey> knownInstances) {
        Objects.requireNonNull(knownInstances, "The knownInstances must not be null.");
        ignoredUntil.keySet().retainAll(knownInstances);
        delegate.retainOnly(knownInstances);
    }
}
