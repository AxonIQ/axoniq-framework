/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.testcontainer;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Constructs an Axon Server container for testing.
 * <p>
 * By default, it starts in a single-node configuration.
 *
 * @author Lucas Campos
 * @author Steven van Beelen
 * @author 4.8.0
 */
public class AxonServerContainer extends GenericContainer<AxonServerContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(AxonServerContainer.class);

    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("docker.axoniq.io/axoniq/axonserver");

    private static final int AXON_SERVER_HTTP_PORT = 8024;
    private static final int AXON_SERVER_GRPC_PORT = 8124;

    private static final String WAIT_FOR_LOG_MESSAGE = ".*Started AxonServer.*";
    private static final String HEALTH_ENDPOINT = "/actuator/health";

    private static final String LICENCE_DEFAULT_LOCATION = "/axonserver/config/axoniq.license";
    private static final String CONFIGURATION_DEFAULT_LOCATION = "/axonserver/config/axonserver.properties";
    private static final String CLUSTER_TEMPLATE_DEFAULT_LOCATION = "/axonserver/cluster-template.yml";

    private static final String AXONIQ_LICENSE = "AXONIQ_LICENSE";
    private static final String AXONIQ_AXONSERVER_NAME = "AXONIQ_AXONSERVER_NAME";
    private static final String AXONIQ_AXONSERVER_INTERNAL_HOSTNAME = "AXONIQ_AXONSERVER_INTERNAL_HOSTNAME";
    private static final String AXONIQ_AXONSERVER_HOSTNAME = "AXONIQ_AXONSERVER_HOSTNAME";
    private static final String AXONIQ_AXONSERVER_DEVMODE_ENABLED = "AXONIQ_AXONSERVER_DEVMODE_ENABLED";
    private static final String AXONIQ_AXONSERVER_PLUGINS_ENABLED = "AXONIQ_AXONSERVER_PLUGINS_ENABLED";
    private static final String AXONIQ_AXONSERVER_REPLICATION_MIN_ELECTION_TIMEOUT =
            "AXONIQ_AXONSERVER_REPLICATION_MIN_ELECTION_TIMEOUT";
    private static final String AXONIQ_AXONSERVER_REPLICATION_MAX_ELECTION_TIMEOUT =
            "AXONIQ_AXONSERVER_REPLICATION_MAX_ELECTION_TIMEOUT";
    private static final String AXONIQ_AXONSERVER_REPLICATION_HEARTBEAT_TIMEOUT =
            "AXONIQ_AXONSERVER_REPLICATION_HEARTBEAT_TIMEOUT";

    private static final String AXON_SERVER_ADDRESS_TEMPLATE = "%s:%s";

    /**
     * Startup timeout for both wait strategies below. A container that could still become ready given a bit more time
     * -- e.g. under CPU contention from other concurrently-starting containers on a busy CI runner -- should not fail
     * the build just because the default 60s Testcontainers timeout was too tight.
     */
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);

    /**
     * Threshold above which a container's actual startup time is logged, to surface slow boots (e.g. caused by CPU
     * contention from other concurrently-starting containers) without logging on every normal, fast start.
     */
    private static final Duration SLOW_STARTUP_THRESHOLD = Duration.ofSeconds(30);

    private String licensePath;
    private @Nullable String licenseContent;
    private String configurationPath;
    private String clusterTemplatePath;
    private String axonServerName;
    private String axonServerInternalHostname;
    private String axonServerHostname;
    private boolean devMode;
    private boolean dcbContext;

    /**
     * Initialize an Axon Server {@link GenericContainer test container} using the default image name
     * {@code "docker.axoniq.io/axoniq/axonserver"}.
     */
    public AxonServerContainer() {
        this(DEFAULT_IMAGE_NAME);
    }

    /**
     * Initialize Axon Server with the given {@code dockerImageName}.
     *
     * @param dockerImageName The name of the Docker image to initialize this test container with.
     */
    public AxonServerContainer(final String dockerImageName) {
        this(DockerImageName.parse(dockerImageName));
    }

    /**
     * Initialize Axon Server with the given {@code dockerImageName}.
     *
     * @param dockerImageName The {@link DockerImageName} to initialize this test container with.
     */
    public AxonServerContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);

        dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME, DockerImageName.parse("axoniq/axonserver"));

        //noinspection resource | ignore from AutoClosable on GenericContainer
        withExposedPorts(AXON_SERVER_HTTP_PORT, AXON_SERVER_GRPC_PORT)
                .withEnv("TESTCONTAINERS_FORK_NUMBER", "" + System.getProperty("test.forkNumber", "0"))
                .withEnv(AXONIQ_LICENSE, LICENCE_DEFAULT_LOCATION)
                // Skip OSGi plugin loading entirely: framework tests never install plugins.
                .withEnv(AXONIQ_AXONSERVER_PLUGINS_ENABLED, "false")
                .waitingFor(Wait.forLogMessage(WAIT_FOR_LOG_MESSAGE, 1).withStartupTimeout(STARTUP_TIMEOUT))
                .waitingFor(Wait.forHttp(HEALTH_ENDPOINT)
                                .forPort(AXON_SERVER_HTTP_PORT)
                                .withStartupTimeout(STARTUP_TIMEOUT));
    }

    @Override
    protected void configure() {
        if (licensePath != null && licenseContent != null) {
            LOG.warn("Both a license path and license content are configured; configure only one of them.");
        }
        if (licenseContent != null) {
            //noinspection resource | ignore from AutoClosable on GenericContainer
            withCopyToContainer(Transferable.of(licenseContent), LICENCE_DEFAULT_LOCATION);
        } else {
            optionallyCopyResourceToContainer(LICENCE_DEFAULT_LOCATION, licensePath);
        }
        optionallyCopyResourceToContainer(CONFIGURATION_DEFAULT_LOCATION, configurationPath);
        optionallyCopyResourceToContainer(CLUSTER_TEMPLATE_DEFAULT_LOCATION, clusterTemplatePath);
        withOptionalEnv(AXONIQ_AXONSERVER_NAME, axonServerName);
        withOptionalEnv(AXONIQ_AXONSERVER_HOSTNAME, axonServerHostname);
        withOptionalEnv(AXONIQ_AXONSERVER_INTERNAL_HOSTNAME, axonServerInternalHostname);
        //noinspection resource | ignore from AutoClosable on GenericContainer
        withEnv(AXONIQ_AXONSERVER_DEVMODE_ENABLED, String.valueOf(devMode));
        if (clusterTemplatePath == null) {
            // Shrink Raft election/heartbeat timeouts: safe for the single-member replication groups a
            // container without a cluster template runs (no real quorum risk), and shaves a guaranteed 1-2s+
            // off every boot. Left untouched when a cluster template is supplied, since a real multi-node
            // cluster needs Axon Server's own production-safe timing to avoid spurious re-elections.
            //noinspection resource | ignore from AutoClosable on GenericContainer
            withEnv(AXONIQ_AXONSERVER_REPLICATION_MIN_ELECTION_TIMEOUT, "50")
                    .withEnv(AXONIQ_AXONSERVER_REPLICATION_MAX_ELECTION_TIMEOUT, "200")
                    .withEnv(AXONIQ_AXONSERVER_REPLICATION_HEARTBEAT_TIMEOUT, "50");
        }
    }

    @Override
    protected void doStart() {
        Instant startingAt = Instant.now();
        super.doStart();
        Duration startupDuration = Duration.between(startingAt, Instant.now());
        if (startupDuration.compareTo(SLOW_STARTUP_THRESHOLD) > 0) {
            LOG.warn("Axon Server container [{}] took {} to become ready",
                     getContainerName().replaceFirst("^/", ""), startupDuration);
        }
        try {
            AxonServerContainerUtils.initCluster(getHost(), getHttpPort(), isShouldBeReused(), dcbContext);
        } catch (IOException e) {
            throw new ContainerLaunchException("Axon Server cluster initialization failed.", e);
        }
    }

    /**
     * Map (effectively replace) a directory in Docker with the content of {@code resourceLocation} if the resource
     * location is not {@code null}.
     * <p>
     * Protected to allow for changing implementation by extending the class.
     *
     * @param pathNameInContainer The path in docker.
     * @param resourceLocation    The relative classpath to the resource.
     */
    protected void optionallyCopyResourceToContainer(String pathNameInContainer, String resourceLocation) {
        //noinspection resource | ignore from AutoClosable on GenericContainer
        Optional.ofNullable(resourceLocation)
                .map(MountableFile::forClasspathResource)
                .ifPresent(mountableFile -> withCopyFileToContainer(mountableFile, pathNameInContainer));
    }

    /**
     * Set an environment value if the value is present.
     * <p>
     * Protected to allow for changing implementation by extending the class
     *
     * @param key   Environment value key, usually a constant.
     * @param value Environment value to be set.
     */
    protected void withOptionalEnv(String key, String value) {
        //noinspection resource | ignore from AutoClosable on GenericContainer
        Optional.ofNullable(value)
                .ifPresent(v -> withEnv(key, value));
    }

    /**
     * Initialize this Axon Server test container with a license file retrieved from the given {@code licensePath}.
     *
     * @param licensePath The path to the license file.
     * @return This container itself for fluent API.
     */
    public AxonServerContainer withLicense(String licensePath) {
        this.licensePath = licensePath;
        return self();
    }

    /**
     * Initialize this Axon Server test container with the given license {@code licenseContent}, rather than a license
     * file on the classpath as {@link #withLicense(String)} does.
     * <p>
     * Useful when the license is provided as a value instead of a file, for example through the {@code AXONIQ_LICENSE}
     * environment variable on a CI server. The content is written to the license location inside the container on
     * startup. When both a license path and license content are configured, the license content takes precedence.
     *
     * @param licenseContent the content of the license, or {@code null} to start without a license
     * @return this container itself for fluent API
     * @since 5.5.0
     */
    public AxonServerContainer withLicenseContent(@Nullable String licenseContent) {
        this.licenseContent = licenseContent;
        return self();
    }

    /**
     * Initialize this Axon Server test container with a configuration file retrieved from the given
     * {@code configurationPath}.
     *
     * @param configurationPath The path to the configuration file.
     * @return This container itself for fluent API.
     */
    public AxonServerContainer withConfiguration(String configurationPath) {
        this.configurationPath = configurationPath;
        return self();
    }

    /**
     * Initialize this Axon Server test container with a cluster template configuration file retrieved from the given
     * {@code clusterTemplatePath}.
     *
     * @param clusterTemplatePath The path to the cluster template file.
     * @return This container itself for fluent API.
     */
    public AxonServerContainer withClusterTemplate(String clusterTemplatePath) {
        this.clusterTemplatePath = clusterTemplatePath;
        return self();
    }

    /**
     * Initialize this Axon Server test container with the given {@code axonServerName}.
     *
     * @param axonServerName The name of the Axon Server instance.
     * @return This container itself for fluent API.
     */
    public AxonServerContainer withAxonServerName(String axonServerName) {
        this.axonServerName = axonServerName;
        return self();
    }

    /**
     * Initialize this Axon Server test container with the given {@code axonServerInternalHostname}.
     *
     * @param axonServerInternalHostname The internal hostname of the Axon Server instance.
     * @return This container itself for fluent API.
     */
    public AxonServerContainer withAxonServerInternalHostname(String axonServerInternalHostname) {
        this.axonServerInternalHostname = axonServerInternalHostname;
        return self();
    }

    /**
     * Initialize this Axon Server test container with the given {@code axonServerHostname}.
     *
     * @param axonServerHostname The hostname of the Axon Server instance.
     * @return This container itself for fluent API.
     */
    public AxonServerContainer withAxonServerHostname(String axonServerHostname) {
        this.axonServerHostname = axonServerHostname;
        return self();
    }

    /**
     * Initialize this Axon Server test container with the given {@code devMode}.
     * <p>
     * Development mode enables some features for development convenience. Default value is {@code false}.
     *
     * @param devMode A {@code boolean} dictating whether to enable development mode, yes or no.
     * @return This container itself for fluent API.
     */
    public AxonServerContainer withDevMode(boolean devMode) {
        this.devMode = devMode;
        return self();
    }

    /**
     * Initialize this Axon Server test container with DCB (Dynamic Consistency Boundary) context support.
     * <p>
     * When enabled, the default context will be created with DCB support, allowing for more flexible consistency
     * boundaries in event-sourced applications. DCB enables defining consistency boundaries dynamically based on event
     * tags rather than being limited to aggregate-based boundaries.
     * <p>
     * Default value is {@code false}.
     *
     * @param dcbContext A {@code boolean} dictating whether to enable DCB context support, yes or no.
     * @return This container itself for fluent API.
     */
    public AxonServerContainer withDcbContext(boolean dcbContext) {
        this.dcbContext = dcbContext;
        return self();
    }

    /**
     * Returns whether this container was configured with DCB context support.
     *
     * @return {@code true} if DCB context support is enabled, {@code false} otherwise
     */
    public boolean isDcbContext() {
        return dcbContext;
    }

    /**
     * Returns the mapped Http port used by this Axon Server container.
     *
     * @return The mapped Http port used by this Axon Server container.
     */
    public Integer getHttpPort() {
        return this.getMappedPort(AXON_SERVER_HTTP_PORT);
    }

    /**
     * Returns the mapped gRPC port used by this Axon Server container.
     *
     * @return The mapped gRPC port used by this Axon Server container.
     */
    public Integer getGrpcPort() {
        return this.getMappedPort(AXON_SERVER_GRPC_PORT);
    }

    /**
     * Returns the container address in a {@code host:port} format.
     *
     * @return The container address in a {@code host:port} format.
     */
    public String getAxonServerAddress() {
        return String.format(AXON_SERVER_ADDRESS_TEMPLATE,
                             this.getHost(),
                             this.getMappedPort(AXON_SERVER_GRPC_PORT));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        AxonServerContainer that = (AxonServerContainer) o;
        return Objects.equals(licensePath, that.licensePath)
                && Objects.equals(licenseContent, that.licenseContent)
                && Objects.equals(configurationPath, that.configurationPath)
                && Objects.equals(clusterTemplatePath, that.clusterTemplatePath)
                && Objects.equals(axonServerName, that.axonServerName)
                && Objects.equals(axonServerInternalHostname, that.axonServerInternalHostname)
                && Objects.equals(axonServerHostname, that.axonServerHostname)
                && Objects.equals(devMode, that.devMode)
                && Objects.equals(dcbContext, that.dcbContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(),
                            licensePath,
                            licenseContent,
                            configurationPath,
                            clusterTemplatePath,
                            axonServerName,
                            axonServerInternalHostname,
                            axonServerHostname,
                            devMode,
                            dcbContext);
    }

    @Override
    public String toString() {
        return "AxonServerContainer{" +
                "licensePath='" + licensePath + '\'' +
                ", licenseContent='" + (licenseContent == null ? "[unset]" : "[set]") + '\'' +
                ", configurationPath='" + configurationPath + '\'' +
                ", clusterTemplatePath='" + clusterTemplatePath + '\'' +
                ", axonServerName='" + axonServerName + '\'' +
                ", axonServerInternalHostname='" + axonServerInternalHostname + '\'' +
                ", axonServerHostname='" + axonServerHostname + '\'' +
                ", devMode='" + devMode + '\'' +
                ", dcbContext='" + dcbContext + '\'' +
                '}';
    }
}
