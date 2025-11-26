/*
 * Copyright (c) 2022-2025. AxonIQ B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.platform.framework;

import org.axonframework.common.BuilderUtils;

import java.lang.management.ManagementFactory;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Builder class to instantiate a {@link AxoniqPlatformConfigurerEnhancer}.
 */
public class AxoniqPlatformConfiguration {

    private final String environmentId;
    private final String accessToken;
    private final String applicationName;

    private String host = "framework.console.axoniq.io";
    private Boolean secure = true;
    private Integer port = 7000;
    private String hostname = ManagementFactory.getRuntimeMXBean().getName();
    private String nodeId = randomNodeId();
    private Long initialDelay = 0L;

    private ScheduledExecutorService reportingTaskExecutor;
    private Integer reportingThreadPoolSize = 2;

    /**
     * Constructor to instantiate a {@link AxoniqPlatformConfiguration} based on the fields contained in the
     * {@link AxoniqPlatformConfiguration}. Requires the {@code environmentId}, {@code accessToken} and
     * {@code applicationName} to be set.
     *
     * @param environmentId   The environment identifier of Axoniq Platform.
     * @param accessToken     The access token needed to authenticate to the environment.
     * @param applicationName The display name of the application. Some special characters may be replaced with a
     *                        hyphen.
     */
    public AxoniqPlatformConfiguration(String environmentId, String accessToken, String applicationName) {
        BuilderUtils.assertNonEmpty(environmentId, "Axoniq Platform environmentId may not be null or empty");
        BuilderUtils.assertNonEmpty(accessToken, "Axoniq Platform accessToken may not be null or empty");
        BuilderUtils.assertNonEmpty(applicationName, "Axoniq Platform applicationName may not be null or empty");
        this.environmentId = environmentId;
        this.accessToken = accessToken;
        this.applicationName = applicationName;
    }

    /**
     * The hostname of the application. Defaults to the runtime name of the JVM. Needs to be unique in combination with
     * the {@code nodeId}.
     *
     * @param hostname The hostname of the application
     * @return The builder for fluent interfacing
     */
    public AxoniqPlatformConfiguration hostname(String hostname) {
        BuilderUtils.assertNonEmpty(hostname, "Hostname may not be null or empty");
        this.hostname = hostname;
        return this;
    }

    /**
     * The node id of the application. Defaults to a random four character string. This is to prevent conflicts when
     * multiple instances of the same application are running on the same host.
     *
     * @param nodeId The node id of the application
     * @return The builder for fluent interfacing
     */
    public AxoniqPlatformConfiguration nodeId(String nodeId) {
        BuilderUtils.assertNonEmpty(nodeId, "Node id may not be null or empty");
        this.nodeId = nodeId;
        return this;
    }

    /**
     * The host to connect to. Defaults to {@code framework.console.axoniq.io}.
     *
     * @param host The host to connect to
     * @return The builder for fluent interfacing
     */
    public AxoniqPlatformConfiguration host(String host) {
        BuilderUtils.assertNonEmpty(host, "Axoniq Platform host may not be null or empty");
        this.host = host;
        return this;
    }

    /**
     * The port to connect to. Defaults to {@code 7000}.
     *
     * @param port The port to connect to
     * @return The builder for fluent interfacing
     */
    public AxoniqPlatformConfiguration port(Integer port) {
        BuilderUtils.assertNonNull(host, "Axoniq Platform port may not be null");
        this.port = port;
        return this;
    }

    /**
     * The initial delay before attempting to establish a connection. Defaults to {@code 0}.
     *
     * @param initialDelay The delay in milliseconds
     * @return The builder for fluent interfacing
     */
    public AxoniqPlatformConfiguration initialDelay(Long initialDelay) {
        BuilderUtils.assertPositive(initialDelay, "Axoniq Platform initialDelay must be positive");
        this.initialDelay = initialDelay;
        return this;
    }

    /**
     * The thread pool's size that is used for reporting tasks, such as sending metrics to Platform. Defaults to
     * {@code 2}.
     *
     * @param reportingThreadPoolSize The thread pool size
     * @return The builder for fluent interfacing
     */
    public AxoniqPlatformConfiguration reportingThreadPoolSize(Integer reportingThreadPoolSize) {
        BuilderUtils.assertPositive(reportingThreadPoolSize,
                                    "Axoniq Platform reportingThreadPoolSize must be positive");
        this.reportingThreadPoolSize = reportingThreadPoolSize;
        return this;
    }

    /**
     * The {@link ScheduledExecutorService} that should be used for reporting metrics. Defaults to a
     * {@link Executors#newScheduledThreadPool(int)} with the {@code threadPoolSize} of this builder if not set.
     *
     * @param executorService The executor service.
     * @return The builder for fluent interfacing
     */
    public AxoniqPlatformConfiguration reportingTaskExecutor(ScheduledExecutorService executorService) {
        BuilderUtils.assertNonNull(reportingTaskExecutor, "Axoniq Platform reportingTaskExecutor must be non-null");
        this.reportingTaskExecutor = executorService;
        return this;
    }

    /**
     * Whether to use a secure connection using SSL/TLS or not. Defaults to {@code true}.
     *
     * @param secure Whether to use a secure connection or not
     * @return The builder for fluent interfacing
     */
    public AxoniqPlatformConfiguration secure(boolean secure) {
        this.secure = secure;
        return this;
    }

    public ScheduledExecutorService getReportingTaskExecutor() {
        if (reportingTaskExecutor == null) {
            reportingTaskExecutor = Executors.newScheduledThreadPool(reportingThreadPoolSize);
        }
        return reportingTaskExecutor;
    }

    public String getEnvironmentId() {
        return environmentId;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getApplicationName() {
        return applicationName.replaceAll("([\\[\\]])", "-");
    }

    public String getInstanceName() {
        return getHostname() + "-" + getNodeId();
    }

    public String getHost() {
        return host;
    }

    public Boolean getSecure() {
        return secure;
    }

    public Integer getPort() {
        return port;
    }

    public String getHostname() {
        return hostname;
    }

    public String getNodeId() {
        return nodeId;
    }

    public Long getInitialDelay() {
        return initialDelay;
    }

    private String randomNodeId() {
        String uuid = UUID.randomUUID().toString();
        return uuid.substring(uuid.length() - 4);
    }
}
