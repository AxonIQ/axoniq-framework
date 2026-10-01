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

package io.axoniq.framework.springcloud.transport;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration of the {@link SpringCloudQueryController}.
 * <p>
 * Holds the settings that decide how long the streams the controller answers with may stay open, and how often an
 * idle subscription is written to. For a default configuration use the {@link #DEFAULT} constant, and adjust it
 * through the fluent methods, each of which returns a new instance.
 * <p>
 * The Spring Boot autoconfiguration builds this from the {@code axon.springcloud.query-timeout} and
 * {@code axon.springcloud.subscription-keep-alive-interval} properties.
 * <p>
 * Example usage:
 * <pre>{@code
 * SpringCloudQueryControllerConfiguration configuration =
 *         SpringCloudQueryControllerConfiguration.DEFAULT.queryTimeout(Duration.ofMinutes(1))
 *                                                        .keepAliveInterval(Duration.ofSeconds(10));
 * }</pre>
 *
 * @param queryTimeout      how long a response stream may stay open before the container closes it. A query still
 *                          being answered when it elapses is reported to the member that asked as a failed stream.
 *                          Does not apply to a subscription query, which lasts as long as the subscriber wants it to
 * @param keepAliveInterval how often an idle subscription is written to, so that neither the subscribing member nor the
 *                          intermediaries between the two mistake a quiet subscription for a dead one. Must be
 *                          comfortably below the window subscribing members give a subscription to say something
 * @author Allard Buijze
 * @author Mateusz Nowak
 * @since 5.4.0
 */
public record SpringCloudQueryControllerConfiguration(
        Duration queryTimeout,
        Duration keepAliveInterval
) {

    /**
     * How long a response stream may stay open when no other timeout is configured.
     */
    public static final Duration DEFAULT_QUERY_TIMEOUT = Duration.ofMinutes(5);

    /**
     * How often an idle subscription is written to when no other interval is configured.
     * <p>
     * Comfortably inside the sixty seconds a load balancer, proxy or NAT table commonly gives an idle connection, so
     * that several keep-alives pass before any of them would reclaim it.
     */
    public static final Duration DEFAULT_KEEP_ALIVE_INTERVAL = Duration.ofSeconds(20);

    /**
     * A default instance of the {@link SpringCloudQueryControllerConfiguration}, setting the {@link #queryTimeout()} to
     * {@link #DEFAULT_QUERY_TIMEOUT} and the {@link #keepAliveInterval()} to {@link #DEFAULT_KEEP_ALIVE_INTERVAL}.
     */
    public static final SpringCloudQueryControllerConfiguration DEFAULT =
            new SpringCloudQueryControllerConfiguration(DEFAULT_QUERY_TIMEOUT, DEFAULT_KEEP_ALIVE_INTERVAL);

    /**
     * Compact constructor validating that the given {@code queryTimeout} and {@code keepAliveInterval} are not
     * {@code null}, and that the {@code keepAliveInterval} is positive.
     */
    @SuppressWarnings("MissingJavadoc")
    public SpringCloudQueryControllerConfiguration {
        Objects.requireNonNull(queryTimeout, "The queryTimeout must not be null.");
        Objects.requireNonNull(keepAliveInterval, "The keepAliveInterval must not be null.");
        if (keepAliveInterval.isNegative() || keepAliveInterval.isZero()) {
            throw new IllegalArgumentException(
                    "The keep-alive interval must be positive, but was [" + keepAliveInterval + "]."
            );
        }
    }

    /**
     * Sets how long a response stream may stay open before the container closes it.
     * <p>
     * Defaults to {@link #DEFAULT_QUERY_TIMEOUT}.
     *
     * @param queryTimeout how long a response stream may stay open before the container closes it
     * @return a new configuration instance, for fluent interfacing
     */
    public SpringCloudQueryControllerConfiguration queryTimeout(Duration queryTimeout) {
        return new SpringCloudQueryControllerConfiguration(queryTimeout, this.keepAliveInterval);
    }

    /**
     * Sets how often an idle subscription is written to.
     * <p>
     * Defaults to {@link #DEFAULT_KEEP_ALIVE_INTERVAL}.
     *
     * @param keepAliveInterval how often an idle subscription is written to, must be positive
     * @return a new configuration instance, for fluent interfacing
     */
    public SpringCloudQueryControllerConfiguration keepAliveInterval(Duration keepAliveInterval) {
        return new SpringCloudQueryControllerConfiguration(this.queryTimeout, keepAliveInterval);
    }
}
