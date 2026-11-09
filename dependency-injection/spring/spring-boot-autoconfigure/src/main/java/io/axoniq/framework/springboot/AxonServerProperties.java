/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Spring Boot configuration properties for the Axon Server connector, bound to the {@code axon.axonserver} prefix.
 * <p>
 * This class is the Spring counterpart of {@link AxonServerConfiguration}, which holds no Spring dependency itself.
 * Every property mirrors the {@link AxonServerConfiguration} property of the same name, and each default is taken
 * from a fresh {@link AxonServerConfiguration} so that both can never drift apart.
 * <p>
 * Use {@link #toConfiguration()} to obtain the {@link AxonServerConfiguration} that is made available as a bean by
 * {@link io.axoniq.framework.springboot.autoconfig.AxonServerAutoConfiguration}. Setting values that are not
 * explicitly provided, such as the flow control settings, are left unset on that configuration so its derived
 * defaults still apply.
 *
 * @author Steven van Beelen
 * @since 5.5.0
 */
@ConfigurationProperties("axon.axonserver")
public class AxonServerProperties {

    private static final AxonServerConfiguration DEFAULTS = new AxonServerConfiguration();
    private static final AxonServerConfiguration.PersistentStreamSettings DEFAULT_STREAM =
            new AxonServerConfiguration.PersistentStreamSettings();

    /**
     * Whether (automatic) configuration of the Axon Server Connector is enabled.
     * <p>
     * See {@link AxonServerConfiguration#isEnabled()} for the default and further details.
     */
    private boolean enabled = DEFAULTS.isEnabled();

    /**
     * The comma separated list of Axon Server servers, where each element is a hostname or hostname:grpcPort.
     * <p>
     * See {@link AxonServerConfiguration#getServers()} for the default and further details.
     */
    private String servers = DEFAULTS.getServers();

    /**
     * The client identifier as it registers itself to Axon Server.
     * <p>
     * See {@link AxonServerConfiguration#getClientId()} for the default and further details.
     */
    private String clientId = DEFAULTS.getClientId();

    /**
     * The name of this application.
     * <p>
     * See {@link AxonServerConfiguration#getComponentName()} for the default and further details.
     */
    private @Nullable String componentName;

    /**
     * The token providing access control with Axon Server.
     * <p>
     * See {@link AxonServerConfiguration#getToken()} for the default and further details.
     */
    private @Nullable String token;

    /**
     * The bounded context that this application operates in.
     * <p>
     * See {@link AxonServerConfiguration#getContext()} for the default and further details.
     */
    private String context = DEFAULTS.getContext();

    /**
     * The path to the certificate file used for TLS.
     * <p>
     * See {@link AxonServerConfiguration#getCertFile()} for the default and further details.
     */
    private @Nullable String certFile;

    /**
     * The path to the client certificate (chain) file, in PEM format, used for mutual TLS.
     * <p>
     * See {@link AxonServerConfiguration#getClientCertFile()} for the default and further details.
     */
    private @Nullable String clientCertFile;

    /**
     * The path to the client's private key file, in PKCS#8 PEM format, used for mutual TLS.
     * <p>
     * See {@link AxonServerConfiguration#getClientKeyFile()} for the default and further details.
     */
    private @Nullable String clientKeyFile;

    /**
     * Whether to use TLS for the connection to Axon Server.
     * <p>
     * See {@link AxonServerConfiguration#isSslEnabled()} for the default and further details.
     */
    private boolean sslEnabled = DEFAULTS.isSslEnabled();

    /**
     * The initial number of permits sent for message streams (events, commands, queries).
     * <p>
     * See {@link AxonServerConfiguration#getPermits()} for the default and further details.
     */
    private Integer permits = DEFAULTS.getPermits();

    /**
     * The number of additional permits sent for message streams when the application is ready for more messages.
     * <p>
     * See {@link AxonServerConfiguration#getNrOfNewPermits()} for the default and further details.
     */
    private @Nullable Integer nrOfNewPermits;

    /**
     * The threshold at which the application sends new permits to the server.
     * <p>
     * See {@link AxonServerConfiguration#getNewPermitsThreshold()} for the default and further details.
     */
    private @Nullable Integer newPermitsThreshold;

    /**
     * The specific flow control settings for the event message stream.
     * <p>
     * See {@link AxonServerConfiguration#getEventFlowControl()} for the default and further details.
     */
    private @Nullable FlowControlProperties eventFlowControl;

    /**
     * The specific flow control settings for the query message stream.
     * <p>
     * See {@link AxonServerConfiguration#getQueryFlowControl()} for the default and further details.
     */
    private @Nullable FlowControlProperties queryFlowControl;

    /**
     * The specific flow control settings for the command message stream.
     * <p>
     * See {@link AxonServerConfiguration#getCommandFlowControl()} for the default and further details.
     */
    private @Nullable FlowControlProperties commandFlowControl;

    /**
     * Whether queries should be shortcut to local handlers when available, bypassing remote dispatch.
     * <p>
     * See {@link AxonServerConfiguration#isShortcutQueriesToLocalHandlers()} for the default and further details.
     */
    private boolean shortcutQueriesToLocalHandlers = DEFAULTS.isShortcutQueriesToLocalHandlers();

    /**
     * The number of threads executing commands.
     * <p>
     * See {@link AxonServerConfiguration#getCommandThreads()} for the default and further details.
     */
    private int commandThreads = DEFAULTS.getCommandThreads();

    /**
     * The number of threads executing queries.
     * <p>
     * See {@link AxonServerConfiguration#getQueryThreads()} for the default and further details.
     */
    private @Nullable Integer queryThreads;

    /**
     * The number of threads executing query responses.
     * <p>
     * See {@link AxonServerConfiguration#getQueryResponseThreads()} for the default and further details.
     */
    @SuppressWarnings("removal")
    private int queryResponseThreads = DEFAULTS.getQueryResponseThreads();

    /**
     * The interval in milliseconds at which the application sends status updates on event processors to Axon Server.
     * <p>
     * See {@link AxonServerConfiguration#getProcessorsNotificationRate()} for the default and further details.
     */
    private int processorsNotificationRate = DEFAULTS.getProcessorsNotificationRate();

    /**
     * The initial delay in milliseconds before the application sends its first status update on event processors to
     * Axon Server.
     * <p>
     * See {@link AxonServerConfiguration#getProcessorsNotificationInitialDelay()} for the default and further
     * details.
     */
    private int processorsNotificationInitialDelay = DEFAULTS.getProcessorsNotificationInitialDelay();

    /**
     * The timeout in milliseconds for keep alive requests.
     * <p>
     * See {@link AxonServerConfiguration#getKeepAliveTimeout()} for the default and further details.
     */
    private long keepAliveTimeout = DEFAULTS.getKeepAliveTimeout();

    /**
     * The interval in milliseconds for keep alive requests, where 0 disables keep alive.
     * <p>
     * See {@link AxonServerConfiguration#getKeepAliveTime()} for the default and further details.
     */
    private long keepAliveTime = DEFAULTS.getKeepAliveTime();

    /**
     * The maximum number of aggregate snapshots to retrieve.
     * <p>
     * See {@link AxonServerConfiguration#getSnapshotPrefetch()} for the default and further details.
     */
    private int snapshotPrefetch = DEFAULTS.getSnapshotPrefetch();

    /**
     * The gRPC max inbound message size.
     * <p>
     * See {@link AxonServerConfiguration#getMaxMessageSize()} for the default and further details.
     */
    private int maxMessageSize = DEFAULTS.getMaxMessageSize();

    /**
     * The threshold, as a percentage between 0 and 1 of the max message size, at which a warning is logged.
     * <p>
     * See {@link AxonServerConfiguration#getMaxMessageSizeWarningThreshold()} for the default and further details.
     */
    private double maxMessageSizeWarningThreshold = DEFAULTS.getMaxMessageSizeWarningThreshold();

    /**
     * The load factor of this client for command routing.
     * <p>
     * See {@link AxonServerConfiguration#getCommandLoadFactor()} for the default and further details.
     */
    private int commandLoadFactor = DEFAULTS.getCommandLoadFactor();

    /**
     * The maximum time in milliseconds a request for the initial Axon Server connection may last.
     * <p>
     * See {@link AxonServerConfiguration#getConnectTimeout()} for the default and further details.
     */
    private long connectTimeout = DEFAULTS.getConnectTimeout();

    /**
     * The time in milliseconds to wait between attempts to connect to Axon Server.
     * <p>
     * See {@link AxonServerConfiguration#getReconnectInterval()} for the default and further details.
     */
    private long reconnectInterval = DEFAULTS.getReconnectInterval();

    /**
     * Whether events may only be read from the leader node instead of the node the client is connected to.
     * <p>
     * See {@link AxonServerConfiguration#isForceReadFromLeader()} for the default and further details.
     */
    private boolean forceReadFromLeader = DEFAULTS.isForceReadFromLeader();

    /**
     * Whether the connection should always be re-established through the configured servers.
     * <p>
     * See {@link AxonServerConfiguration#isForceReconnectThroughServers()} for the default and further details.
     */
    private boolean forceReconnectThroughServers = DEFAULTS.isForceReconnectThroughServers();

    /**
     * The number of threads used for connection management activities.
     * <p>
     * See {@link AxonServerConfiguration#getConnectionManagementThreadPoolSize()} for the default and further
     * details.
     */
    private int connectionManagementThreadPoolSize = DEFAULTS.getConnectionManagementThreadPoolSize();

    /**
     * The settings for sending heartbeat messages to ensure a fully operational end-to-end connection with Axon
     * Server.
     * <p>
     * See {@link AxonServerConfiguration#getHeartbeat()} for the default and further details.
     */
    private HeartbeatProperties heartbeat = new HeartbeatProperties();

    /**
     * The settings for event handling of this application on Axon Server.
     * <p>
     * See {@link AxonServerConfiguration#getEventhandling()} for the default and further details.
     */
    private EventHandlingProperties eventhandling = new EventHandlingProperties();

    /**
     * The settings for the Axon Server Event Store of this application.
     * <p>
     * See {@link AxonServerConfiguration#getEventStore()} for the default and further details.
     */
    private EventStoreProperties eventStore = new EventStoreProperties();

    /**
     * The settings for each of the configured persistent streams, by name.
     * <p>
     * See {@link AxonServerConfiguration#getPersistentStreams()} for the default and further details.
     */
    private Map<String, PersistentStreamProperties> persistentStreams = new HashMap<>();

    /**
     * Whether all event processors should default to persistent streams.
     * <p>
     * See {@link AxonServerConfiguration#isAutoPersistentStreamsEnabled()} for the default and further details.
     */
    private boolean autoPersistentStreamsEnabled = DEFAULTS.isAutoPersistentStreamsEnabled();

    /**
     * The settings for all persistent streams that are created automatically.
     * <p>
     * See {@link AxonServerConfiguration#getAutoPersistentStreamsSettings()} for the default and further details.
     */
    private PersistentStreamProperties autoPersistentStreamsSettings = new PersistentStreamProperties();

    /**
     * Returns whether (automatic) configuration of the Axon Server Connector is enabled.
     *
     * @return whether (automatic) configuration of the Axon Server Connector is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets whether (automatic) configuration of the Axon Server Connector is enabled.
     *
     * @param enabled whether (automatic) configuration of the Axon Server Connector is enabled
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Returns the comma separated list of Axon Server servers, where each element is a hostname or hostname:grpcPort.
     *
     * @return the comma separated list of Axon Server servers, where each element is a hostname or hostname:grpcPort
     */
    public String getServers() {
        return servers;
    }

    /**
     * Sets the comma separated list of Axon Server servers, where each element is a hostname or hostname:grpcPort.
     *
     * @param servers the comma separated list of Axon Server servers, where each element is a hostname or
     *         hostname:grpcPort
     */
    public void setServers(String servers) {
        this.servers = servers;
    }

    /**
     * Returns the client identifier as it registers itself to Axon Server.
     *
     * @return the client identifier as it registers itself to Axon Server
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Sets the client identifier as it registers itself to Axon Server.
     *
     * @param clientId the client identifier as it registers itself to Axon Server
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * Returns the name of this application.
     *
     * @return the name of this application
     */
    public @Nullable String getComponentName() {
        return componentName;
    }

    /**
     * Sets the name of this application.
     *
     * @param componentName the name of this application
     */
    public void setComponentName(@Nullable String componentName) {
        this.componentName = componentName;
    }

    /**
     * Returns the token providing access control with Axon Server.
     *
     * @return the token providing access control with Axon Server
     */
    public @Nullable String getToken() {
        return token;
    }

    /**
     * Sets the token providing access control with Axon Server.
     *
     * @param token the token providing access control with Axon Server
     */
    public void setToken(@Nullable String token) {
        this.token = token;
    }

    /**
     * Returns the bounded context that this application operates in.
     *
     * @return the bounded context that this application operates in
     */
    public String getContext() {
        return context;
    }

    /**
     * Sets the bounded context that this application operates in.
     *
     * @param context the bounded context that this application operates in
     */
    public void setContext(String context) {
        this.context = context;
    }

    /**
     * Returns the path to the certificate file used for TLS.
     *
     * @return the path to the certificate file used for TLS
     */
    public @Nullable String getCertFile() {
        return certFile;
    }

    /**
     * Sets the path to the certificate file used for TLS.
     *
     * @param certFile the path to the certificate file used for TLS
     */
    public void setCertFile(@Nullable String certFile) {
        this.certFile = certFile;
    }

    /**
     * Returns the path to the client certificate (chain) file, in PEM format, used for mutual TLS.
     *
     * @return the path to the client certificate (chain) file, in PEM format, used for mutual TLS
     */
    public @Nullable String getClientCertFile() {
        return clientCertFile;
    }

    /**
     * Sets the path to the client certificate (chain) file, in PEM format, used for mutual TLS.
     *
     * @param clientCertFile the path to the client certificate (chain) file, in PEM format, used for mutual TLS
     */
    public void setClientCertFile(@Nullable String clientCertFile) {
        this.clientCertFile = clientCertFile;
    }

    /**
     * Returns the path to the client's private key file, in PKCS#8 PEM format, used for mutual TLS.
     *
     * @return the path to the client's private key file, in PKCS#8 PEM format, used for mutual TLS
     */
    public @Nullable String getClientKeyFile() {
        return clientKeyFile;
    }

    /**
     * Sets the path to the client's private key file, in PKCS#8 PEM format, used for mutual TLS.
     *
     * @param clientKeyFile the path to the client's private key file, in PKCS#8 PEM format, used for mutual TLS
     */
    public void setClientKeyFile(@Nullable String clientKeyFile) {
        this.clientKeyFile = clientKeyFile;
    }

    /**
     * Returns whether to use TLS for the connection to Axon Server.
     *
     * @return whether to use TLS for the connection to Axon Server
     */
    public boolean isSslEnabled() {
        return sslEnabled;
    }

    /**
     * Sets whether to use TLS for the connection to Axon Server.
     *
     * @param sslEnabled whether to use TLS for the connection to Axon Server
     */
    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    /**
     * Returns the initial number of permits sent for message streams (events, commands, queries).
     *
     * @return the initial number of permits sent for message streams (events, commands, queries)
     */
    public Integer getPermits() {
        return permits;
    }

    /**
     * Sets the initial number of permits sent for message streams (events, commands, queries).
     *
     * @param permits the initial number of permits sent for message streams (events, commands, queries)
     */
    public void setPermits(Integer permits) {
        this.permits = permits;
    }

    /**
     * Returns the number of additional permits sent for message streams when the application is ready for more
     * messages.
     *
     * @return the number of additional permits sent for message streams when the application is ready for more
     *         messages
     */
    public @Nullable Integer getNrOfNewPermits() {
        return nrOfNewPermits;
    }

    /**
     * Sets the number of additional permits sent for message streams when the application is ready for more messages.
     *
     * @param nrOfNewPermits the number of additional permits sent for message streams when the application is ready
     *         for more messages
     */
    public void setNrOfNewPermits(@Nullable Integer nrOfNewPermits) {
        this.nrOfNewPermits = nrOfNewPermits;
    }

    /**
     * Returns the threshold at which the application sends new permits to the server.
     *
     * @return the threshold at which the application sends new permits to the server
     */
    public @Nullable Integer getNewPermitsThreshold() {
        return newPermitsThreshold;
    }

    /**
     * Sets the threshold at which the application sends new permits to the server.
     *
     * @param newPermitsThreshold the threshold at which the application sends new permits to the server
     */
    public void setNewPermitsThreshold(@Nullable Integer newPermitsThreshold) {
        this.newPermitsThreshold = newPermitsThreshold;
    }

    /**
     * Returns the specific flow control settings for the event message stream.
     *
     * @return the specific flow control settings for the event message stream
     */
    public @Nullable FlowControlProperties getEventFlowControl() {
        return eventFlowControl;
    }

    /**
     * Sets the specific flow control settings for the event message stream.
     *
     * @param eventFlowControl the specific flow control settings for the event message stream
     */
    public void setEventFlowControl(@Nullable FlowControlProperties eventFlowControl) {
        this.eventFlowControl = eventFlowControl;
    }

    /**
     * Returns the specific flow control settings for the query message stream.
     *
     * @return the specific flow control settings for the query message stream
     */
    public @Nullable FlowControlProperties getQueryFlowControl() {
        return queryFlowControl;
    }

    /**
     * Sets the specific flow control settings for the query message stream.
     *
     * @param queryFlowControl the specific flow control settings for the query message stream
     */
    public void setQueryFlowControl(@Nullable FlowControlProperties queryFlowControl) {
        this.queryFlowControl = queryFlowControl;
    }

    /**
     * Returns the specific flow control settings for the command message stream.
     *
     * @return the specific flow control settings for the command message stream
     */
    public @Nullable FlowControlProperties getCommandFlowControl() {
        return commandFlowControl;
    }

    /**
     * Sets the specific flow control settings for the command message stream.
     *
     * @param commandFlowControl the specific flow control settings for the command message stream
     */
    public void setCommandFlowControl(@Nullable FlowControlProperties commandFlowControl) {
        this.commandFlowControl = commandFlowControl;
    }

    /**
     * Returns whether queries should be shortcut to local handlers when available, bypassing remote dispatch.
     *
     * @return whether queries should be shortcut to local handlers when available, bypassing remote dispatch
     */
    public boolean isShortcutQueriesToLocalHandlers() {
        return shortcutQueriesToLocalHandlers;
    }

    /**
     * Sets whether queries should be shortcut to local handlers when available, bypassing remote dispatch.
     *
     * @param shortcutQueriesToLocalHandlers whether queries should be shortcut to local handlers when available,
     *         bypassing remote dispatch
     */
    public void setShortcutQueriesToLocalHandlers(boolean shortcutQueriesToLocalHandlers) {
        this.shortcutQueriesToLocalHandlers = shortcutQueriesToLocalHandlers;
    }

    /**
     * Returns the number of threads executing commands.
     *
     * @return the number of threads executing commands
     */
    public int getCommandThreads() {
        return commandThreads;
    }

    /**
     * Sets the number of threads executing commands.
     *
     * @param commandThreads the number of threads executing commands
     */
    public void setCommandThreads(int commandThreads) {
        this.commandThreads = commandThreads;
    }

    /**
     * Returns the number of threads executing queries.
     *
     * @return the number of threads executing queries
     */
    public @Nullable Integer getQueryThreads() {
        return queryThreads;
    }

    /**
     * Sets the number of threads executing queries.
     *
     * @param queryThreads the number of threads executing queries
     */
    public void setQueryThreads(@Nullable Integer queryThreads) {
        this.queryThreads = queryThreads;
    }

    /**
     * Returns the number of threads executing query responses.
     *
     * @return the number of threads executing query responses
     * @deprecated see {@link AxonServerConfiguration#getQueryResponseThreads()}
     */
    @Deprecated(forRemoval = true)
    public int getQueryResponseThreads() {
        return queryResponseThreads;
    }

    /**
     * Sets the number of threads executing query responses.
     *
     * @param queryResponseThreads the number of threads executing query responses
     * @deprecated see {@link AxonServerConfiguration#getQueryResponseThreads()}
     */
    @Deprecated(forRemoval = true)
    public void setQueryResponseThreads(int queryResponseThreads) {
        this.queryResponseThreads = queryResponseThreads;
    }

    /**
     * Returns the interval in milliseconds at which the application sends status updates on event processors to Axon
     * Server.
     *
     * @return the interval in milliseconds at which the application sends status updates on event processors to Axon
     *         Server
     */
    public int getProcessorsNotificationRate() {
        return processorsNotificationRate;
    }

    /**
     * Sets the interval in milliseconds at which the application sends status updates on event processors to Axon
     * Server.
     *
     * @param processorsNotificationRate the interval in milliseconds at which the application sends status updates on
     *         event processors to Axon Server
     */
    public void setProcessorsNotificationRate(int processorsNotificationRate) {
        this.processorsNotificationRate = processorsNotificationRate;
    }

    /**
     * Returns the initial delay in milliseconds before the application sends its first status update on event
     * processors to Axon Server.
     *
     * @return the initial delay in milliseconds before the application sends its first status update on event
     *         processors to Axon Server
     */
    public int getProcessorsNotificationInitialDelay() {
        return processorsNotificationInitialDelay;
    }

    /**
     * Sets the initial delay in milliseconds before the application sends its first status update on event processors
     * to Axon Server.
     *
     * @param processorsNotificationInitialDelay the initial delay in milliseconds before the application sends its
     *         first status update on event processors to Axon Server
     */
    public void setProcessorsNotificationInitialDelay(int processorsNotificationInitialDelay) {
        this.processorsNotificationInitialDelay = processorsNotificationInitialDelay;
    }

    /**
     * Returns the timeout in milliseconds for keep alive requests.
     *
     * @return the timeout in milliseconds for keep alive requests
     */
    public long getKeepAliveTimeout() {
        return keepAliveTimeout;
    }

    /**
     * Sets the timeout in milliseconds for keep alive requests.
     *
     * @param keepAliveTimeout the timeout in milliseconds for keep alive requests
     */
    public void setKeepAliveTimeout(long keepAliveTimeout) {
        this.keepAliveTimeout = keepAliveTimeout;
    }

    /**
     * Returns the interval in milliseconds for keep alive requests, where 0 disables keep alive.
     *
     * @return the interval in milliseconds for keep alive requests, where 0 disables keep alive
     */
    public long getKeepAliveTime() {
        return keepAliveTime;
    }

    /**
     * Sets the interval in milliseconds for keep alive requests, where 0 disables keep alive.
     *
     * @param keepAliveTime the interval in milliseconds for keep alive requests, where 0 disables keep alive
     */
    public void setKeepAliveTime(long keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
    }

    /**
     * Returns the maximum number of aggregate snapshots to retrieve.
     *
     * @return the maximum number of aggregate snapshots to retrieve
     */
    public int getSnapshotPrefetch() {
        return snapshotPrefetch;
    }

    /**
     * Sets the maximum number of aggregate snapshots to retrieve.
     *
     * @param snapshotPrefetch the maximum number of aggregate snapshots to retrieve
     */
    public void setSnapshotPrefetch(int snapshotPrefetch) {
        this.snapshotPrefetch = snapshotPrefetch;
    }

    /**
     * Returns the gRPC max inbound message size.
     *
     * @return the gRPC max inbound message size
     */
    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    /**
     * Sets the gRPC max inbound message size.
     *
     * @param maxMessageSize the gRPC max inbound message size
     */
    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    /**
     * Returns the threshold, as a percentage between 0 and 1 of the max message size, at which a warning is logged.
     *
     * @return the threshold, as a percentage between 0 and 1 of the max message size, at which a warning is logged
     */
    public double getMaxMessageSizeWarningThreshold() {
        return maxMessageSizeWarningThreshold;
    }

    /**
     * Sets the threshold, as a percentage between 0 and 1 of the max message size, at which a warning is logged.
     *
     * @param maxMessageSizeWarningThreshold the threshold, as a percentage between 0 and 1 of the max message size,
     *         at which a warning is logged
     */
    public void setMaxMessageSizeWarningThreshold(double maxMessageSizeWarningThreshold) {
        this.maxMessageSizeWarningThreshold = maxMessageSizeWarningThreshold;
    }

    /**
     * Returns the load factor of this client for command routing.
     *
     * @return the load factor of this client for command routing
     */
    public int getCommandLoadFactor() {
        return commandLoadFactor;
    }

    /**
     * Sets the load factor of this client for command routing.
     *
     * @param commandLoadFactor the load factor of this client for command routing
     */
    public void setCommandLoadFactor(int commandLoadFactor) {
        this.commandLoadFactor = commandLoadFactor;
    }

    /**
     * Returns the maximum time in milliseconds a request for the initial Axon Server connection may last.
     *
     * @return the maximum time in milliseconds a request for the initial Axon Server connection may last
     */
    public long getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Sets the maximum time in milliseconds a request for the initial Axon Server connection may last.
     *
     * @param connectTimeout the maximum time in milliseconds a request for the initial Axon Server connection may
     *         last
     */
    public void setConnectTimeout(long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Returns the time in milliseconds to wait between attempts to connect to Axon Server.
     *
     * @return the time in milliseconds to wait between attempts to connect to Axon Server
     */
    public long getReconnectInterval() {
        return reconnectInterval;
    }

    /**
     * Sets the time in milliseconds to wait between attempts to connect to Axon Server.
     *
     * @param reconnectInterval the time in milliseconds to wait between attempts to connect to Axon Server
     */
    public void setReconnectInterval(long reconnectInterval) {
        this.reconnectInterval = reconnectInterval;
    }

    /**
     * Returns whether events may only be read from the leader node instead of the node the client is connected to.
     *
     * @return whether events may only be read from the leader node instead of the node the client is connected to
     */
    public boolean isForceReadFromLeader() {
        return forceReadFromLeader;
    }

    /**
     * Sets whether events may only be read from the leader node instead of the node the client is connected to.
     *
     * @param forceReadFromLeader whether events may only be read from the leader node instead of the node the client
     *         is connected to
     */
    public void setForceReadFromLeader(boolean forceReadFromLeader) {
        this.forceReadFromLeader = forceReadFromLeader;
    }

    /**
     * Returns whether the connection should always be re-established through the configured servers.
     *
     * @return whether the connection should always be re-established through the configured servers
     */
    public boolean isForceReconnectThroughServers() {
        return forceReconnectThroughServers;
    }

    /**
     * Sets whether the connection should always be re-established through the configured servers.
     *
     * @param forceReconnectThroughServers whether the connection should always be re-established through the
     *         configured servers
     */
    public void setForceReconnectThroughServers(boolean forceReconnectThroughServers) {
        this.forceReconnectThroughServers = forceReconnectThroughServers;
    }

    /**
     * Returns the number of threads used for connection management activities.
     *
     * @return the number of threads used for connection management activities
     */
    public int getConnectionManagementThreadPoolSize() {
        return connectionManagementThreadPoolSize;
    }

    /**
     * Sets the number of threads used for connection management activities.
     *
     * @param connectionManagementThreadPoolSize the number of threads used for connection management activities
     */
    public void setConnectionManagementThreadPoolSize(int connectionManagementThreadPoolSize) {
        this.connectionManagementThreadPoolSize = connectionManagementThreadPoolSize;
    }

    /**
     * Returns the settings for sending heartbeat messages to ensure a fully operational end-to-end connection with
     * Axon Server.
     *
     * @return the settings for sending heartbeat messages to ensure a fully operational end-to-end connection with
     *         Axon Server
     */
    public HeartbeatProperties getHeartbeat() {
        return heartbeat;
    }

    /**
     * Sets the settings for sending heartbeat messages to ensure a fully operational end-to-end connection with Axon
     * Server.
     *
     * @param heartbeat the settings for sending heartbeat messages to ensure a fully operational end-to-end
     *         connection with Axon Server
     */
    public void setHeartbeat(HeartbeatProperties heartbeat) {
        this.heartbeat = heartbeat;
    }

    /**
     * Returns the settings for event handling of this application on Axon Server.
     *
     * @return the settings for event handling of this application on Axon Server
     */
    public EventHandlingProperties getEventhandling() {
        return eventhandling;
    }

    /**
     * Sets the settings for event handling of this application on Axon Server.
     *
     * @param eventhandling the settings for event handling of this application on Axon Server
     */
    public void setEventhandling(EventHandlingProperties eventhandling) {
        this.eventhandling = eventhandling;
    }

    /**
     * Returns the settings for the Axon Server Event Store of this application.
     *
     * @return the settings for the Axon Server Event Store of this application
     */
    public EventStoreProperties getEventStore() {
        return eventStore;
    }

    /**
     * Sets the settings for the Axon Server Event Store of this application.
     *
     * @param eventStore the settings for the Axon Server Event Store of this application
     */
    public void setEventStore(EventStoreProperties eventStore) {
        this.eventStore = eventStore;
    }

    /**
     * Returns the settings for each of the configured persistent streams, by name.
     *
     * @return the settings for each of the configured persistent streams, by name
     */
    public Map<String, PersistentStreamProperties> getPersistentStreams() {
        return persistentStreams;
    }

    /**
     * Returns whether all event processors should default to persistent streams.
     *
     * @return whether all event processors should default to persistent streams
     */
    public boolean isAutoPersistentStreamsEnabled() {
        return autoPersistentStreamsEnabled;
    }

    /**
     * Sets whether all event processors should default to persistent streams.
     *
     * @param autoPersistentStreamsEnabled whether all event processors should default to persistent streams
     */
    public void setAutoPersistentStreamsEnabled(boolean autoPersistentStreamsEnabled) {
        this.autoPersistentStreamsEnabled = autoPersistentStreamsEnabled;
    }

    /**
     * Returns the settings for all persistent streams that are created automatically.
     *
     * @return the settings for all persistent streams that are created automatically
     */
    public PersistentStreamProperties getAutoPersistentStreamsSettings() {
        return autoPersistentStreamsSettings;
    }

    /**
     * Sets the settings for all persistent streams that are created automatically.
     *
     * @param autoPersistentStreamsSettings the settings for all persistent streams that are created automatically
     */
    public void setAutoPersistentStreamsSettings(PersistentStreamProperties autoPersistentStreamsSettings) {
        this.autoPersistentStreamsSettings = autoPersistentStreamsSettings;
    }

    /**
     * Converts these properties into the {@link AxonServerConfiguration} they mirror.
     * <p>
     * Optional values that were not provided are not set on the returned configuration, so that the derived defaults
     * of {@link AxonServerConfiguration} apply.
     *
     * @return a new {@link AxonServerConfiguration} holding the values of these properties
     */
    @SuppressWarnings("removal")
    public AxonServerConfiguration toConfiguration() {
        AxonServerConfiguration configuration = new AxonServerConfiguration();
        configuration.setEnabled(enabled);
        configuration.setServers(servers);
        configuration.setClientId(clientId);
        if (componentName != null) {
            configuration.setComponentName(componentName);
        }
        if (token != null) {
            configuration.setToken(token);
        }
        configuration.setContext(context);
        if (certFile != null) {
            configuration.setCertFile(certFile);
        }
        if (clientCertFile != null) {
            configuration.setClientCertFile(clientCertFile);
        }
        if (clientKeyFile != null) {
            configuration.setClientKeyFile(clientKeyFile);
        }
        configuration.setSslEnabled(sslEnabled);
        configuration.setPermits(permits);
        if (nrOfNewPermits != null) {
            configuration.setNrOfNewPermits(nrOfNewPermits);
        }
        if (newPermitsThreshold != null) {
            configuration.setNewPermitsThreshold(newPermitsThreshold);
        }
        if (eventFlowControl != null) {
            configuration.setEventFlowControl(eventFlowControl.toFlowControlConfiguration());
        }
        if (queryFlowControl != null) {
            configuration.setQueryFlowControl(queryFlowControl.toFlowControlConfiguration());
        }
        if (commandFlowControl != null) {
            configuration.setCommandFlowControl(commandFlowControl.toFlowControlConfiguration());
        }
        configuration.setShortcutQueriesToLocalHandlers(shortcutQueriesToLocalHandlers);
        configuration.setCommandThreads(commandThreads);
        if (queryThreads != null) {
            configuration.setQueryThreads(queryThreads);
        }
        configuration.setQueryResponseThreads(queryResponseThreads);
        configuration.setProcessorsNotificationRate(processorsNotificationRate);
        configuration.setProcessorsNotificationInitialDelay(processorsNotificationInitialDelay);
        configuration.setKeepAliveTimeout(keepAliveTimeout);
        configuration.setKeepAliveTime(keepAliveTime);
        configuration.setSnapshotPrefetch(snapshotPrefetch);
        configuration.setMaxMessageSize(maxMessageSize);
        configuration.setMaxMessageSizeWarningThreshold(maxMessageSizeWarningThreshold);
        configuration.setCommandLoadFactor(commandLoadFactor);
        configuration.setConnectTimeout(connectTimeout);
        configuration.setReconnectInterval(reconnectInterval);
        configuration.setForceReadFromLeader(forceReadFromLeader);
        configuration.setForceReconnectThroughServers(forceReconnectThroughServers);
        configuration.setConnectionManagementThreadPoolSize(connectionManagementThreadPoolSize);
        configuration.getHeartbeat().setEnabled(heartbeat.isEnabled());
        configuration.getHeartbeat().setInterval(heartbeat.getInterval());
        configuration.getHeartbeat().setTimeout(heartbeat.getTimeout());
        eventhandling.getProcessors().forEach((name, processor) -> {
            AxonServerConfiguration.Eventhandling.ProcessorSettings settings =
                    new AxonServerConfiguration.Eventhandling.ProcessorSettings();
            settings.setLoadBalancingStrategy(processor.getLoadBalancingStrategy());
            settings.setAutomaticBalancing(processor.isAutomaticBalancing());
            configuration.getEventhandling().getProcessors().put(name, settings);
        });
        configuration.getEventStore().setEnabled(eventStore.isEnabled());
        persistentStreams.forEach((name, stream) -> configuration.getPersistentStreams()
                                                                  .put(name, toPersistentStreamSettings(stream)));
        configuration.setAutoPersistentStreamsEnabled(autoPersistentStreamsEnabled);
        configuration.setAutoPersistentStreamsSettings(toPersistentStreamSettings(autoPersistentStreamsSettings));
        return configuration;
    }

    private static AxonServerConfiguration.PersistentStreamSettings toPersistentStreamSettings(
            PersistentStreamProperties properties
    ) {
        AxonServerConfiguration.PersistentStreamSettings settings =
                new AxonServerConfiguration.PersistentStreamSettings();
        settings.setInitialSegmentCount(properties.getInitialSegmentCount());
        settings.setThreadCount(properties.getThreadCount());
        settings.setSequencingPolicy(properties.getSequencingPolicy());
        settings.setSequencingPolicyParameters(new ArrayList<>(properties.getSequencingPolicyParameters()));
        if (properties.getFilter() != null) {
            settings.setFilter(properties.getFilter());
        }
        if (properties.getName() != null) {
            settings.setName(properties.getName());
        }
        settings.setBatchSize(properties.getBatchSize());
        settings.setInitialPosition(properties.getInitialPosition());
        return settings;
    }

    /**
     * Flow control settings for a specific message stream, bound to {@link
     * AxonServerConfiguration.FlowControlConfiguration}.
     */
    public static class FlowControlProperties {

        /**
         * The initial number of permits sent for this message stream.
         * <p>
         * See {@link AxonServerConfiguration.FlowControlConfiguration#getPermits()} for the default and further
         * details.
         */
        private @Nullable Integer permits;

        /**
         * The number of additional permits sent when the application is ready for more messages.
         * <p>
         * See {@link AxonServerConfiguration.FlowControlConfiguration#getNrOfNewPermits()} for the default and
         * further details.
         */
        private @Nullable Integer nrOfNewPermits;

        /**
         * The threshold at which the application sends new permits to the server.
         * <p>
         * See {@link AxonServerConfiguration.FlowControlConfiguration#getNewPermitsThreshold()} for the default and
         * further details.
         */
        private @Nullable Integer newPermitsThreshold;

        /**
         * Returns the initial number of permits sent for this message stream.
         *
         * @return the initial number of permits sent for this message stream
         */
        public @Nullable Integer getPermits() {
            return permits;
        }

        /**
         * Sets the initial number of permits sent for this message stream.
         *
         * @param permits the initial number of permits sent for this message stream
         */
        public void setPermits(@Nullable Integer permits) {
            this.permits = permits;
        }

        /**
         * Returns the number of additional permits sent when the application is ready for more messages.
         *
         * @return the number of additional permits sent when the application is ready for more messages
         */
        public @Nullable Integer getNrOfNewPermits() {
            return nrOfNewPermits;
        }

        /**
         * Sets the number of additional permits sent when the application is ready for more messages.
         *
         * @param nrOfNewPermits the number of additional permits sent when the application is ready for more messages
         */
        public void setNrOfNewPermits(@Nullable Integer nrOfNewPermits) {
            this.nrOfNewPermits = nrOfNewPermits;
        }

        /**
         * Returns the threshold at which the application sends new permits to the server.
         *
         * @return the threshold at which the application sends new permits to the server
         */
        public @Nullable Integer getNewPermitsThreshold() {
            return newPermitsThreshold;
        }

        /**
         * Sets the threshold at which the application sends new permits to the server.
         *
         * @param newPermitsThreshold the threshold at which the application sends new permits to the server
         */
        public void setNewPermitsThreshold(@Nullable Integer newPermitsThreshold) {
            this.newPermitsThreshold = newPermitsThreshold;
        }

        /**
         * Converts these properties into a {@link AxonServerConfiguration.FlowControlConfiguration}.
         *
         * @return a {@link AxonServerConfiguration.FlowControlConfiguration} holding the values of these properties
         */
        AxonServerConfiguration.FlowControlConfiguration toFlowControlConfiguration() {
            return new AxonServerConfiguration.FlowControlConfiguration(permits, nrOfNewPermits, newPermitsThreshold);
        }
    }

    /**
     * Heartbeat settings, bound to {@link AxonServerConfiguration.HeartbeatConfiguration}.
     */
    public static class HeartbeatProperties {

        /**
         * Whether heartbeat messages between the client and Axon Server are enabled.
         * <p>
         * See {@link AxonServerConfiguration.HeartbeatConfiguration#isEnabled()} for the default and further details.
         */
        private boolean enabled = DEFAULTS.getHeartbeat().isEnabled();

        /**
         * The interval between consecutive heartbeat messages in milliseconds.
         * <p>
         * See {@link AxonServerConfiguration.HeartbeatConfiguration#getInterval()} for the default and further
         * details.
         */
        private long interval = DEFAULTS.getHeartbeat().getInterval();

        /**
         * The time window in milliseconds within which a heartbeat response is expected.
         * <p>
         * See {@link AxonServerConfiguration.HeartbeatConfiguration#getTimeout()} for the default and further
         * details.
         */
        private long timeout = DEFAULTS.getHeartbeat().getTimeout();

        /**
         * Returns whether heartbeat messages between the client and Axon Server are enabled.
         *
         * @return whether heartbeat messages between the client and Axon Server are enabled
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Sets whether heartbeat messages between the client and Axon Server are enabled.
         *
         * @param enabled whether heartbeat messages between the client and Axon Server are enabled
         */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * Returns the interval between consecutive heartbeat messages in milliseconds.
         *
         * @return the interval between consecutive heartbeat messages in milliseconds
         */
        public long getInterval() {
            return interval;
        }

        /**
         * Sets the interval between consecutive heartbeat messages in milliseconds.
         *
         * @param interval the interval between consecutive heartbeat messages in milliseconds
         */
        public void setInterval(long interval) {
            this.interval = interval;
        }

        /**
         * Returns the time window in milliseconds within which a heartbeat response is expected.
         *
         * @return the time window in milliseconds within which a heartbeat response is expected
         */
        public long getTimeout() {
            return timeout;
        }

        /**
         * Sets the time window in milliseconds within which a heartbeat response is expected.
         *
         * @param timeout the time window in milliseconds within which a heartbeat response is expected
         */
        public void setTimeout(long timeout) {
            this.timeout = timeout;
        }
    }

    /**
     * Event handling settings, bound to {@link AxonServerConfiguration.Eventhandling}.
     */
    public static class EventHandlingProperties {

        /**
         * The settings for each of the configured event processors, by name.
         * <p>
         * See {@link AxonServerConfiguration.Eventhandling#getProcessors()} for the default and further details.
         */
        private Map<String, ProcessorProperties> processors = new HashMap<>();

        /**
         * Returns the settings for each of the configured event processors, by name.
         *
         * @return the settings for each of the configured event processors, by name
         */
        public Map<String, ProcessorProperties> getProcessors() {
            return processors;
        }

        /**
         * Settings of a single event processor, bound to {@link
         * AxonServerConfiguration.Eventhandling.ProcessorSettings}.
         */
        public static class ProcessorProperties {

            /**
             * The load balancing strategy for the event processor.
             * <p>
             * See {@link AxonServerConfiguration.Eventhandling.ProcessorSettings#getLoadBalancingStrategy()} for the
             * default and further details.
             */
            private String loadBalancingStrategy = "disabled";

            /**
             * Whether the event processor is balanced automatically.
             * <p>
             * See {@link AxonServerConfiguration.Eventhandling.ProcessorSettings#isAutomaticBalancing()} for the
             * default and further details.
             */
            private boolean automaticBalancing = false;

            /**
             * Returns the load balancing strategy for the event processor.
             *
             * @return the load balancing strategy for the event processor
             */
            public String getLoadBalancingStrategy() {
                return loadBalancingStrategy;
            }

            /**
             * Sets the load balancing strategy for the event processor.
             *
             * @param loadBalancingStrategy the load balancing strategy for the event processor
             */
            public void setLoadBalancingStrategy(String loadBalancingStrategy) {
                this.loadBalancingStrategy = loadBalancingStrategy;
            }

            /**
             * Returns whether the event processor is balanced automatically.
             *
             * @return whether the event processor is balanced automatically
             */
            public boolean isAutomaticBalancing() {
                return automaticBalancing;
            }

            /**
             * Sets whether the event processor is balanced automatically.
             *
             * @param automaticBalancing whether the event processor is balanced automatically
             */
            public void setAutomaticBalancing(boolean automaticBalancing) {
                this.automaticBalancing = automaticBalancing;
            }
        }
    }

    /**
     * Axon Server Event Store settings, bound to {@link AxonServerConfiguration.EventStoreConfiguration}.
     */
    public static class EventStoreProperties {

        /**
         * Whether (automatic) configuration of the Axon Server Event Store is enabled.
         * <p>
         * See {@link AxonServerConfiguration.EventStoreConfiguration#isEnabled()} for the default and further
         * details.
         */
        private boolean enabled = DEFAULTS.getEventStore().isEnabled();

        /**
         * Returns whether (automatic) configuration of the Axon Server Event Store is enabled.
         *
         * @return whether (automatic) configuration of the Axon Server Event Store is enabled
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Sets whether (automatic) configuration of the Axon Server Event Store is enabled.
         *
         * @param enabled whether (automatic) configuration of the Axon Server Event Store is enabled
         */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    /**
     * Settings of a single persistent stream, bound to {@link AxonServerConfiguration.PersistentStreamSettings}.
     */
    public static class PersistentStreamProperties {

        /**
         * The number of segments for the persistent stream if it needs to be created.
         * <p>
         * See {@link AxonServerConfiguration.PersistentStreamSettings#getInitialSegmentCount()} for the default and
         * further details.
         */
        private int initialSegmentCount = DEFAULT_STREAM.getInitialSegmentCount();

        /**
         * The number of threads used to process tasks for the persistent stream.
         * <p>
         * See {@link AxonServerConfiguration.PersistentStreamSettings#getThreadCount()} for the default and further
         * details.
         */
        private int threadCount = DEFAULT_STREAM.getThreadCount();

        /**
         * The sequencing policy to use for the persistent stream.
         * <p>
         * See {@link AxonServerConfiguration.PersistentStreamSettings#getSequencingPolicy()} for the default and
         * further details.
         */
        private String sequencingPolicy = DEFAULT_STREAM.getSequencingPolicy();

        /**
         * The parameters specified for the sequencing policy.
         * <p>
         * See {@link AxonServerConfiguration.PersistentStreamSettings#getSequencingPolicyParameters()} for the
         * default and further details.
         */
        private List<String> sequencingPolicyParameters =
                new ArrayList<>(DEFAULT_STREAM.getSequencingPolicyParameters());

        /**
         * The expression to filter out events in the persistent stream, using the Axon Server Query Language.
         * <p>
         * See {@link AxonServerConfiguration.PersistentStreamSettings#getFilter()} for the default and further
         * details.
         */
        private @Nullable String filter;

        /**
         * An optional override for the stream name on Axon Server.
         * <p>
         * See {@link AxonServerConfiguration.PersistentStreamSettings#getName()} for the default and further details.
         */
        private @Nullable String name;

        /**
         * The maximum number of events to process in a single transaction when reading the persistent stream.
         * <p>
         * See {@link AxonServerConfiguration.PersistentStreamSettings#getBatchSize()} for the default and further
         * details.
         */
        private int batchSize = DEFAULT_STREAM.getBatchSize();

        /**
         * The initial token position for the persistent stream.
         * <p>
         * See {@link AxonServerConfiguration.PersistentStreamSettings#getInitialPosition()} for the default and
         * further details.
         */
        private String initialPosition = DEFAULT_STREAM.getInitialPosition();

        /**
         * Returns the number of segments for the persistent stream if it needs to be created.
         *
         * @return the number of segments for the persistent stream if it needs to be created
         */
        public int getInitialSegmentCount() {
            return initialSegmentCount;
        }

        /**
         * Sets the number of segments for the persistent stream if it needs to be created.
         *
         * @param initialSegmentCount the number of segments for the persistent stream if it needs to be created
         */
        public void setInitialSegmentCount(int initialSegmentCount) {
            this.initialSegmentCount = initialSegmentCount;
        }

        /**
         * Returns the number of threads used to process tasks for the persistent stream.
         *
         * @return the number of threads used to process tasks for the persistent stream
         */
        public int getThreadCount() {
            return threadCount;
        }

        /**
         * Sets the number of threads used to process tasks for the persistent stream.
         *
         * @param threadCount the number of threads used to process tasks for the persistent stream
         */
        public void setThreadCount(int threadCount) {
            this.threadCount = threadCount;
        }

        /**
         * Returns the sequencing policy to use for the persistent stream.
         *
         * @return the sequencing policy to use for the persistent stream
         */
        public String getSequencingPolicy() {
            return sequencingPolicy;
        }

        /**
         * Sets the sequencing policy to use for the persistent stream.
         *
         * @param sequencingPolicy the sequencing policy to use for the persistent stream
         */
        public void setSequencingPolicy(String sequencingPolicy) {
            this.sequencingPolicy = sequencingPolicy;
        }

        /**
         * Returns the parameters specified for the sequencing policy.
         *
         * @return the parameters specified for the sequencing policy
         */
        public List<String> getSequencingPolicyParameters() {
            return sequencingPolicyParameters;
        }

        /**
         * Sets the parameters specified for the sequencing policy.
         *
         * @param sequencingPolicyParameters the parameters specified for the sequencing policy
         */
        public void setSequencingPolicyParameters(List<String> sequencingPolicyParameters) {
            this.sequencingPolicyParameters = sequencingPolicyParameters;
        }

        /**
         * Returns the expression to filter out events in the persistent stream, using the Axon Server Query Language.
         *
         * @return the expression to filter out events in the persistent stream, using the Axon Server Query Language
         */
        public @Nullable String getFilter() {
            return filter;
        }

        /**
         * Sets the expression to filter out events in the persistent stream, using the Axon Server Query Language.
         *
         * @param filter the expression to filter out events in the persistent stream, using the Axon Server Query
         *         Language
         */
        public void setFilter(@Nullable String filter) {
            this.filter = filter;
        }

        /**
         * Returns an optional override for the stream name on Axon Server.
         *
         * @return an optional override for the stream name on Axon Server
         */
        public @Nullable String getName() {
            return name;
        }

        /**
         * Sets an optional override for the stream name on Axon Server.
         *
         * @param name an optional override for the stream name on Axon Server
         */
        public void setName(@Nullable String name) {
            this.name = name;
        }

        /**
         * Returns the maximum number of events to process in a single transaction when reading the persistent stream.
         *
         * @return the maximum number of events to process in a single transaction when reading the persistent stream
         */
        public int getBatchSize() {
            return batchSize;
        }

        /**
         * Sets the maximum number of events to process in a single transaction when reading the persistent stream.
         *
         * @param batchSize the maximum number of events to process in a single transaction when reading the
         *         persistent stream
         */
        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        /**
         * Returns the initial token position for the persistent stream.
         *
         * @return the initial token position for the persistent stream
         */
        public String getInitialPosition() {
            return initialPosition;
        }

        /**
         * Sets the initial token position for the persistent stream.
         *
         * @param initialPosition the initial token position for the persistent stream
         */
        public void setInitialPosition(String initialPosition) {
            this.initialPosition = initialPosition;
        }
    }
}
