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

package io.axoniq.framework.springboot.springcloud;

import io.axoniq.framework.springcloud.discovery.RestCapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.transport.HttpRemoteCommandDispatcher;
import io.axoniq.framework.springcloud.transport.HttpRemoteQueryDispatcher;
import io.axoniq.framework.springcloud.transport.SpringCloudCommandController;
import io.axoniq.framework.springcloud.transport.SpringCloudQueryController;
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
     * When set to {@code false}, no connector is registered and neither commands nor queries are distributed through
     * Spring Cloud. Defaults to {@code true}.
     */
    private boolean enabled = true;

    /**
     * The path this application receives commands from other members under.
     * <p>
     * Must match {@link #capabilitiesEndpoint} in one respect: every member of the cluster has to agree on it.
     * Defaults to {@link SpringCloudCommandController#DEFAULT_COMMAND_ENDPOINT}.
     */
    private String commandEndpoint = SpringCloudCommandController.DEFAULT_COMMAND_ENDPOINT;

    /**
     * The path this application serves its command handling capabilities under.
     * <p>
     * Every member of the cluster has to agree on it. Defaults to
     * {@link RestCapabilityDiscoveryMode#DEFAULT_CAPABILITIES_ENDPOINT}.
     */
    private String capabilitiesEndpoint = RestCapabilityDiscoveryMode.DEFAULT_CAPABILITIES_ENDPOINT;

    /**
     * The path this application receives queries from other members under, and expects to reach them on.
     * <p>
     * Every member must agree on this path, as it is how they reach each other. Defaults to
     * {@link SpringCloudQueryController#DEFAULT_QUERY_ENDPOINT}.
     */
    private String queryEndpoint = SpringCloudQueryController.DEFAULT_QUERY_ENDPOINT;

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
     * How long a query's response stream may stay open before the container closes it.
     * <p>
     * A query still being answered when this elapses is reported to the member that asked as a failed stream, so this
     * should exceed the time the slowest query legitimately takes to answer. Defaults to five minutes.
     */
    private Duration queryTimeout = Duration.ofMinutes(5);

    /**
     * How long this application waits for the responses to a query it dispatched, before giving up on it.
     * <p>
     * A backstop rather than the usual way a query ends: the answering member closes its own stream first, on
     * {@link #queryTimeout}. This one covers the member that stops answering without saying so — one that was killed,
     * or partitioned away — whose socket would otherwise never report anything. Keep it above {@code queryTimeout},
     * so that a member which is merely slow ends the query itself. Defaults to
     * {@link HttpRemoteQueryDispatcher#DEFAULT_RESPONSE_TIMEOUT}.
     */
    private Duration queryResponseTimeout = HttpRemoteQueryDispatcher.DEFAULT_RESPONSE_TIMEOUT;

    /**
     * How many responses to a single query this application buffers while consuming them.
     * <p>
     * A member answering faster than this application consumes fills the buffer, and the query then fails rather than
     * growing the buffer until memory runs out. Defaults to
     * {@link HttpRemoteQueryDispatcher#DEFAULT_BUFFER_SIZE}.
     */
    private int queryBufferSize = HttpRemoteQueryDispatcher.DEFAULT_BUFFER_SIZE;

    /**
     * How often this application writes to a subscription query it is answering while it has no update to send.
     * <p>
     * A subscription may go a long time without an update, and an idle connection is what a load balancer, proxy or
     * NAT table reclaims. Keeping it comfortably below the idle timeout of whatever sits between members -- sixty
     * seconds, commonly -- is what stops a healthy subscription being cut. Defaults to
     * {@link SpringCloudQueryController#DEFAULT_KEEP_ALIVE_INTERVAL}.
     */
    private Duration subscriptionKeepAliveInterval = SpringCloudQueryController.DEFAULT_KEEP_ALIVE_INTERVAL;

    /**
     * How long a subscription this application opened may hear nothing at all before it is given up on.
     * <p>
     * Not a deadline on the subscription, which lasts as long as the subscriber wants it to, but on silence. The
     * answering member writes a keep-alive every {@link #subscriptionKeepAliveInterval}, so hearing nothing for the
     * whole of this window means that member is gone rather than merely quiet. Keep it a few keep-alives wide.
     * Defaults to {@link HttpRemoteQueryDispatcher#DEFAULT_SUBSCRIPTION_INACTIVITY_TIMEOUT}.
     */
    private Duration subscriptionInactivityTimeout =
            HttpRemoteQueryDispatcher.DEFAULT_SUBSCRIPTION_INACTIVITY_TIMEOUT;

    /**
     * How long a service instance is left alone after answering a capabilities request with a client error.
     * <p>
     * Spring Cloud Discovery reports every registered service, not only those running this connector. An instance that
     * answers with a client error is not serving the capabilities endpoint, and asking it again on every heartbeat is
     * wasted work. The period is finite because an instance may have answered that way only because it was still
     * starting up. Defaults to one minute.
     */
    private Duration ignoreListingExpireThreshold = Duration.ofMinutes(1);

    /**
     * The service instance metadata property holding an instance's context root, appended to its URI when reaching it.
     * <p>
     * Leave unset when services are served from the root, which is the default and is what {@code null} means here.
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
     * Returns the path this application receives queries from other members under.
     *
     * @return the path this application receives queries under
     */
    public String getQueryEndpoint() {
        return queryEndpoint;
    }

    /**
     * Sets the path this application receives queries from other members under.
     *
     * @param queryEndpoint the path this application receives queries under
     */
    public void setQueryEndpoint(String queryEndpoint) {
        this.queryEndpoint = queryEndpoint;
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
     * Returns how long a query's response stream may stay open.
     *
     * @return how long a query's response stream may stay open
     */
    public Duration getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Sets how long a query's response stream may stay open.
     *
     * @param queryTimeout how long a query's response stream may stay open
     */
    public void setQueryTimeout(Duration queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    /**
     * Returns how long this application waits for the responses to a query it dispatched.
     *
     * @return how long the responses to a dispatched query are waited for
     */
    public Duration getQueryResponseTimeout() {
        return queryResponseTimeout;
    }

    /**
     * Sets how long this application waits for the responses to a query it dispatched.
     *
     * @param queryResponseTimeout how long the responses to a dispatched query are waited for
     */
    public void setQueryResponseTimeout(Duration queryResponseTimeout) {
        this.queryResponseTimeout = queryResponseTimeout;
    }

    /**
     * Returns how often an idle subscription query is written to.
     *
     * @return how often an idle subscription query is written to
     */
    public Duration getSubscriptionKeepAliveInterval() {
        return subscriptionKeepAliveInterval;
    }

    /**
     * Sets how often an idle subscription query is written to.
     *
     * @param subscriptionKeepAliveInterval how often an idle subscription query is written to
     */
    public void setSubscriptionKeepAliveInterval(Duration subscriptionKeepAliveInterval) {
        this.subscriptionKeepAliveInterval = subscriptionKeepAliveInterval;
    }

    /**
     * Returns how long a subscription may hear nothing before it is given up on.
     *
     * @return how long a subscription may hear nothing before it is given up on
     */
    public Duration getSubscriptionInactivityTimeout() {
        return subscriptionInactivityTimeout;
    }

    /**
     * Sets how long a subscription may hear nothing before it is given up on.
     *
     * @param subscriptionInactivityTimeout how long a subscription may hear nothing before it is given up on
     */
    public void setSubscriptionInactivityTimeout(Duration subscriptionInactivityTimeout) {
        this.subscriptionInactivityTimeout = subscriptionInactivityTimeout;
    }

    /**
     * Returns how many responses to a single query this application buffers.
     *
     * @return how many responses to a single query this application buffers
     */
    public int getQueryBufferSize() {
        return queryBufferSize;
    }

    /**
     * Sets how many responses to a single query this application buffers.
     *
     * @param queryBufferSize how many responses to a single query this application buffers
     */
    public void setQueryBufferSize(int queryBufferSize) {
        this.queryBufferSize = queryBufferSize;
    }

    /**
     * Returns how long a service instance is left alone after answering a capabilities request with a client error.
     *
     * @return the period an instance answering with a client error is ignored for
     */
    public Duration getIgnoreListingExpireThreshold() {
        return ignoreListingExpireThreshold;
    }

    /**
     * Sets how long a service instance is left alone after answering a capabilities request with a client error.
     *
     * @param ignoreListingExpireThreshold the period an instance answering with a client error is ignored for
     */
    public void setIgnoreListingExpireThreshold(Duration ignoreListingExpireThreshold) {
        this.ignoreListingExpireThreshold = ignoreListingExpireThreshold;
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
}
