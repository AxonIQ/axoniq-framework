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

package io.axoniq.framework.springboot;

import io.axoniq.framework.springcloud.discovery.IgnoreListingDiscoveryMode;
import io.axoniq.framework.springcloud.discovery.RestCapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.transport.HttpRemoteCommandDispatcher;
import io.axoniq.framework.springcloud.transport.SpringCloudCommandController;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Properties for configuring the Axoniq Framework Spring Cloud connector.
 * <p>
 * These properties are bound to the {@code axon.springcloud} prefix.
 * <p>
 * The two endpoint paths must be the same on every member of a cluster, since they are how members reach each other.
 * The command load factor is not configured here: it belongs to the distributed command bus as a whole, and is set
 * through {@code DistributedCommandBusConfiguration}.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
@ConfigurationProperties("axon.springcloud")
public class SpringCloudProperties {

    /**
     * Whether the Spring Cloud connector is enabled.
     * <p>
     * When set to {@code false}, no connector is registered and commands are not distributed through Spring Cloud.
     * Defaults to {@code true}.
     */
    private boolean enabled = true;

    /**
     * The path this application receives commands from other members under.
     * <p>
     * Must match {@link #capabilitiesEndpoint} in one respect: every member of the cluster has to agree on it.
     * Defaults to {@code /axoniq-springcloud/command}.
     */
    private String commandEndpoint = SpringCloudCommandController.DEFAULT_COMMAND_ENDPOINT;

    /**
     * The path this application serves its command handling capabilities under.
     * <p>
     * Every member of the cluster has to agree on it. Defaults to
     * {@code /axoniq-springcloud/member-capabilities}.
     */
    private String capabilitiesEndpoint = RestCapabilityDiscoveryMode.DEFAULT_CAPABILITIES_ENDPOINT;

    /**
     * How long a member is given to answer a command before it is treated as unreachable.
     * <p>
     * A member that was killed or partitioned away leaves a socket that reports nothing, which would otherwise hold
     * the dispatch unresolved indefinitely. Reaching this deadline is reported as a failure to reach the member, so
     * the member is taken out of the routing ring until the next discovery round. Defaults to
     * {@link HttpRemoteCommandDispatcher#DEFAULT_REPLY_TIMEOUT}.
     */
    private Duration commandReplyTimeout = HttpRemoteCommandDispatcher.DEFAULT_REPLY_TIMEOUT;

    /**
     * How long an instance is given to answer a capabilities request, both to connect and to respond.
     * <p>
     * Capabilities are asked for on every discovery heartbeat, so this deadline has to stay well under the heartbeat
     * interval. An instance that accepts a connection and then answers nothing would otherwise hold up the round that
     * rebuilds the routing ring, and with it every member's view of who handles what. Defaults to
     * {@link RestCapabilityDiscoveryMode#DEFAULT_CAPABILITIES_TIMEOUT}.
     */
    private Duration capabilitiesTimeout = RestCapabilityDiscoveryMode.DEFAULT_CAPABILITIES_TIMEOUT;

    /**
     * How long a service instance is left alone after answering a capabilities request with a client error.
     * <p>
     * Spring Cloud Discovery reports every registered service, not only those running this connector. An instance that
     * answers with a client error is not serving the capabilities endpoint, and asking it again on every heartbeat is
     * wasted work. The period is finite because an instance may have answered that way only because it was still
     * starting up. Defaults to {@link IgnoreListingDiscoveryMode#DEFAULT_IGNORE_PERIOD}.
     */
    private Duration ignorePeriod = IgnoreListingDiscoveryMode.DEFAULT_IGNORE_PERIOD;

    /**
     * The service instance metadata property holding an instance's context root, appended to its URI when reaching it.
     * <p>
     * Leave unset when services are served from the root, which is the default.
     */
    private @Nullable String contextRootMetadataPropertyName;

    /**
     * Returns whether the Spring Cloud connector is enabled.
     *
     * @return {@code true} if enabled, {@code false} otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets whether the Spring Cloud connector is enabled.
     *
     * @param enabled {@code true} to enable, {@code false} to disable
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Returns the path this application receives commands from other members under.
     *
     * @return the path commands are received under
     */
    public String getCommandEndpoint() {
        return commandEndpoint;
    }

    /**
     * Sets the path this application receives commands from other members under.
     *
     * @param commandEndpoint the path commands are received under
     */
    public void setCommandEndpoint(String commandEndpoint) {
        this.commandEndpoint = commandEndpoint;
    }

    /**
     * Returns the path this application serves its command handling capabilities under.
     *
     * @return the path capabilities are served under
     */
    public String getCapabilitiesEndpoint() {
        return capabilitiesEndpoint;
    }

    /**
     * Sets the path this application serves its command handling capabilities under.
     *
     * @param capabilitiesEndpoint the path capabilities are served under
     */
    public void setCapabilitiesEndpoint(String capabilitiesEndpoint) {
        this.capabilitiesEndpoint = capabilitiesEndpoint;
    }

    /**
     * Returns how long a member is given to answer a command.
     *
     * @return how long a member is given to answer a command
     */
    public Duration getCommandReplyTimeout() {
        return commandReplyTimeout;
    }

    /**
     * Sets how long a member is given to answer a command.
     *
     * @param commandReplyTimeout how long a member is given to answer a command
     */
    public void setCommandReplyTimeout(Duration commandReplyTimeout) {
        this.commandReplyTimeout = commandReplyTimeout;
    }

    /**
     * Returns how long a service instance is left alone after answering a capabilities request with a client error.
     *
     * @return the period an instance answering with a client error is ignored for
     */
    public Duration getIgnorePeriod() {
        return ignorePeriod;
    }

    /**
     * Sets how long a service instance is left alone after answering a capabilities request with a client error.
     *
     * @param ignorePeriod the period an instance answering with a client error is ignored for
     */
    public void setIgnorePeriod(Duration ignorePeriod) {
        this.ignorePeriod = ignorePeriod;
    }

    /**
     * Returns the service instance metadata property holding an instance's context root.
     *
     * @return the metadata property holding an instance's context root, or {@code null} when services are served from
     * the root
     */
    public @Nullable String getContextRootMetadataPropertyName() {
        return contextRootMetadataPropertyName;
    }

    /**
     * Sets the service instance metadata property holding an instance's context root.
     *
     * @param contextRootMetadataPropertyName the metadata property holding an instance's context root
     */
    public void setContextRootMetadataPropertyName(@Nullable String contextRootMetadataPropertyName) {
        this.contextRootMetadataPropertyName = contextRootMetadataPropertyName;
    }

    /**
     * Returns how long an instance is given to answer a capabilities request.
     *
     * @return the deadline for a capabilities request
     */
    public Duration getCapabilitiesTimeout() {
        return capabilitiesTimeout;
    }

    /**
     * Sets how long an instance is given to answer a capabilities request.
     *
     * @param capabilitiesTimeout the deadline for a capabilities request
     */
    public void setCapabilitiesTimeout(Duration capabilitiesTimeout) {
        this.capabilitiesTimeout = capabilitiesTimeout;
    }
}
