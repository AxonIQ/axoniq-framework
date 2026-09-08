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

package io.axoniq.framework.integrationtests.testsuite.infrastructure;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.testcontainer.AxonServerContainer;
import io.axoniq.framework.testcontainer.AxonServerContainerUtils;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.integrationtests.testsuite.infrastructure.TestInfrastructure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.*;
import static io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonIntegrationTestSupport.disableMultiTenancy;
import static io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonIntegrationTestSupport.isMultiTenancyAvailable;

/**
 * {@link TestInfrastructure} implementation that wires tests against a real Axon Server instance managed by
 * Testcontainers.
 * <p>
 * The underlying {@link AxonServerContainer} is a {@code static final} field, so it is shared across all instances of
 * this class and all leaf test classes that use it. {@link AxonServerContainer#start()} is idempotent — Testcontainers
 * makes it a no-op when the container is already running — so calling {@link #start()} from every {@code @BeforeEach}
 * is safe and cheap after the first test.
 * <p>
 * Leaf test classes should hold a {@code private static final} instance of this class:
 * <pre>{@code
 * private static final TestInfrastructure INFRASTRUCTURE = AxonServerTestInfrastructure.singleTenant();
 *
 * @Override
 * protected TestInfrastructure testInfrastructure() {
 *     return INFRASTRUCTURE;
 * }
 * }</pre>
 * <p>
 * Instances are created through {@link #singleTenant(Consumer...)} or {@link #multiTenant(Consumer...)}, so each suite
 * states whether it exercises multi-tenancy instead of leaving that to be inferred.
 *
 * @since 5.1.0
 */
public final class AxonServerTestInfrastructure implements TestInfrastructure {

    private static final Logger LOG = LoggerFactory.getLogger(AxonServerTestInfrastructure.class);

    public static final String AXON_SERVER_TEST_LICENSE = "axon-server-test.license";
    private static final String MULTI_TENANCY_UTILS =
            "io.axoniq.framework.messaging.multitenancy.MultiTenancyUtils";
    private static final AxonServerContainer CONTAINER =
            new AxonServerContainer("docker.axoniq.io/axoniq/axonserver:latest")
                    .withAxonServerHostname("localhost")
                    .withDevMode(true)
                    .withReuse(true)
                    .withDcbContext(true)
                    .withLicense(licenseExists() ? AXON_SERVER_TEST_LICENSE : null);

    public static boolean licenseExists() {
        try (var resource = AxonServerTestInfrastructure.class.getResourceAsStream("/" + AXON_SERVER_TEST_LICENSE)) {
            return resource != null && resource.read() != -1;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final List<Consumer<ComponentRegistry>> infrastructureConfigurators;

    /**
     * Creates a new {@code AxonServerTestInfrastructure} instance with multi-tenancy switched off.
     * <p>
     * The counterpart of {@link #multiTenant(Consumer...)}. Both are named, so every leaf suite states at its
     * declaration whether it exercises multi-tenancy, rather than inheriting that from a constructor.
     *
     * @param infrastructureConfigurators to be executed when {@link #configureInfrastructure(ComponentRegistry)} is
     *                                    called
     * @return an {@code AxonServerTestInfrastructure} with multi-tenancy switched off
     * @since 5.3.0
     */
    @SafeVarargs
    public static AxonServerTestInfrastructure singleTenant(
            Consumer<ComponentRegistry>... infrastructureConfigurators
    ) {
        return new AxonServerTestInfrastructure(List.of(infrastructureConfigurators), false);
    }

    /**
     * Creates a new {@code AxonServerTestInfrastructure} instance that leaves multi-tenancy active.
     *
     * @param infrastructureConfigurators to be executed when {@link #configureInfrastructure(ComponentRegistry)} is
     *                                    called
     * @return an {@code AxonServerTestInfrastructure} that keeps multi-tenancy active
     * @since 5.3.0
     */
    @SafeVarargs
    public static AxonServerTestInfrastructure multiTenant(
            Consumer<ComponentRegistry>... infrastructureConfigurators
    ) {
        return new AxonServerTestInfrastructure(List.of(infrastructureConfigurators), true);
    }

    /**
     * Creates a new {@code AxonServerTestInfrastructure} instance, prepending the multi-tenancy opt-out to the given
     * configurators unless {@code multiTenancyActive}.
     * <p>
     * The opt-out is folded into the configurator list here rather than applied in
     * {@link #configureInfrastructure(ComponentRegistry)}, so an instance's whole configuration policy is the one field
     * and nothing has to be re-decided per configuration.
     *
     * @param infrastructureConfigurators to be executed when {@link #configureInfrastructure(ComponentRegistry)} is
     *                                    called
     * @param multiTenancyActive          whether to leave multi-tenancy active for the built configurations
     */
    private AxonServerTestInfrastructure(List<Consumer<ComponentRegistry>> infrastructureConfigurators,
                                         boolean multiTenancyActive) {
        if (multiTenancyActive && !isMultiTenancyAvailable()) {
            throw new AxonConfigurationException(
                    "Cannot create a multi-tenant AxonServerTestInfrastructure: "
                            + MULTI_TENANCY_UTILS + " is not on the classpath"
            );
        }

        this.infrastructureConfigurators = multiTenancyActive
                ? List.copyOf(infrastructureConfigurators)
                : Stream.concat(Stream.of(disableMultiTenancy), infrastructureConfigurators.stream()).toList();
    }

    @Override
    public void start() {
        boolean wasRunning = CONTAINER.isRunning();
        CONTAINER.start();
        if (!wasRunning) {
            LOG.info("Axon Server UI at http://localhost:{}", CONTAINER.getHttpPort());
        }
    }

    @Override
    public void configureInfrastructure(ComponentRegistry registry) {
        // builder so we can eventually add more configuration options if needed, without having to change impl.
        AxonServerConfiguration.Builder builder = builder()
                .servers(CONTAINER.getHost() + ":" + CONTAINER.getGrpcPort());

        registry.registerComponent(AxonServerConfiguration.class, c -> builder.build());
        infrastructureConfigurators.forEach(configurator -> configurator.accept(registry));
    }

    @Override
    public void purgeData() {
        LOG.info("Purging events from Axon Server.");
        getContextManager().getContexts().stream().filter(c -> !"_admin".equals(c))
                           .forEach(context -> {
                               try {
                                   AxonServerContainerUtils.purgeEventsFromAxonServer(
                                           CONTAINER.getHost(),
                                           CONTAINER.getHttpPort(),
                                           context,
                                           AxonServerContainerUtils.DCB_CONTEXT,
                                           DEFAULT_REPLICATION_GROUP
                                   );
                               } catch (IOException e) {
                                   throw new RuntimeException("Failed to purge events from Axon Server", e);
                               }
                           });
    }

    @Override
    public void stop() {
        // The container is shared across the JVM (static final, withReuse(true)).
        // Testcontainers + Ryuk handle cleanup on JVM exit; stopping per test would
        // defeat reuse. No-op on purpose.
    }

    public ContextManager getContextManager() {
        return new ContextManager() {
            @Override
            public List<String> getContexts() {
                try {
                    return AxonServerContainerUtils.contexts(CONTAINER.getHost(),
                                                             CONTAINER.getHttpPort());
                } catch (IOException e) {
                    throw new RuntimeException("Failed to list contexts from Axon Server", e);
                }
            }

            @Override
            public void createContext(String name, boolean dcb) {
                try {
                    AxonServerContainerUtils.createContext(CONTAINER.getHost(),
                                                           CONTAINER.getHttpPort(),
                                                           name,
                                                           dcb,
                                                           DEFAULT_REPLICATION_GROUP);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to create context in Axon Server", e);
                }
            }

            @Override
            public void deleteContext(String name) {
                try {
                    AxonServerContainerUtils.deleteContext(CONTAINER.getHost(), CONTAINER.getHttpPort(), name);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to delete context in Axon Server", e);
                }
            }
        };
    }

    /**
     * Utility to manage contexts for an {@code AxonServerTestInfrastructure}
     */
    public interface ContextManager {

        /**
         * List all contexts
         *
         * @return the list of context names
         */
        List<String> getContexts();

        /**
         * Create a new DCB context
         *
         * @param name the context name
         */
        default void createContext(String name) {
            createContext(name, true);
        }

        /**
         * Create a new context
         *
         * @param name the context name
         * @param dcb  flag to indicate if it should be a DCB context
         */
        void createContext(String name, boolean dcb);

        /**
         * Delete a context
         *
         * @param name the context name
         */
        void deleteContext(String name);

        /**
         * Delete all contexts except the default and admin contexts
         */
        default void deleteAllCustomContexts() {
            getContexts().stream()
                         .filter(it -> !Set.of(DEFAULT_CONTEXT, ADMIN_CONTEXT).contains(it))
                         .forEach(this::deleteContext);
        }
    }
}
