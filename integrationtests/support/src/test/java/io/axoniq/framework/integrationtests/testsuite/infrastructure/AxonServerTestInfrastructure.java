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
import io.axoniq.framework.testcontainer.AxonServerContainerUtils;
import io.axoniq.framework.testcontainer.SharedAxonServerContainer;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.integrationtests.testsuite.infrastructure.TestInfrastructure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.*;

/**
 * {@link TestInfrastructure} implementation that wires tests against the build-wide
 * {@link SharedAxonServerContainer}.
 * <p>
 * This class owns no container of its own: it adapts {@link SharedAxonServerContainer#INSTANCE} to the
 * {@link TestInfrastructure}/{@link ContextManager} abstraction the integration test suites use, so the tenant
 * contexts created here collapse onto the same container as every other consumer.
 *
 * @since 5.1.0
 */
public final class AxonServerTestInfrastructure implements TestInfrastructure {

    private static final Logger LOG = LoggerFactory.getLogger(AxonServerTestInfrastructure.class);

    public static boolean licenseExists() {
        return SharedAxonServerContainer.licenseExists();
    }

    private final List<Consumer<ComponentRegistry>> infrastructureConfigurators;

    /**
     * Creates a new {@code AxonServerTestInfrastructure} instance.
     *
     * @param infrastructureConfigurators to be executed when {@link #configureInfrastructure(ComponentRegistry)} is
     *                                    called
     */
    @SafeVarargs
    public AxonServerTestInfrastructure(Consumer<ComponentRegistry>... infrastructureConfigurators) {
        this(Arrays.asList(infrastructureConfigurators));
    }

    /**
     * Creates a new {@code AxonServerTestInfrastructure} instance.
     *
     * @param infrastructureConfigurators to be executed when {@link #configureInfrastructure(ComponentRegistry)} is
     *                                    called
     */
    public AxonServerTestInfrastructure(List<Consumer<ComponentRegistry>> infrastructureConfigurators) {
        this.infrastructureConfigurators = List.copyOf(infrastructureConfigurators);
    }

    @Override
    public void start() {
        SharedAxonServerContainer.ensureStarted();
    }

    @Override
    public void configureInfrastructure(ComponentRegistry registry) {
        // builder so we can eventually add more configuration options if needed, without having to change impl.
        AxonServerConfiguration.Builder builder = builder()
                .servers(SharedAxonServerContainer.INSTANCE.getHost() + ":"
                                 + SharedAxonServerContainer.INSTANCE.getGrpcPort());

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
                                           SharedAxonServerContainer.INSTANCE.getHost(),
                                           SharedAxonServerContainer.INSTANCE.getHttpPort(),
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
        // The container is shared build-wide (SharedAxonServerContainer, withReuse(true)).
        // Testcontainers + Ryuk handle cleanup on JVM exit; stopping per test would
        // defeat reuse. No-op on purpose.
    }

    public ContextManager getContextManager() {
        return new ContextManager() {
            @Override
            public List<String> getContexts() {
                try {
                    return AxonServerContainerUtils.contexts(SharedAxonServerContainer.INSTANCE.getHost(),
                                                             SharedAxonServerContainer.INSTANCE.getHttpPort());
                } catch (IOException e) {
                    throw new RuntimeException("Failed to list contexts from Axon Server", e);
                }
            }

            @Override
            public void createContext(String name, boolean dcb) {
                try {
                    AxonServerContainerUtils.createContext(SharedAxonServerContainer.INSTANCE.getHost(),
                                                           SharedAxonServerContainer.INSTANCE.getHttpPort(),
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
                    AxonServerContainerUtils.deleteContext(SharedAxonServerContainer.INSTANCE.getHost(),
                                                           SharedAxonServerContainer.INSTANCE.getHttpPort(), name);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to delete context in Axon Server", e);
                }
            }

            @Override
            public void emptyContext(String name) {
                try {
                    AxonServerContainerUtils.purgeEventsFromAxonServer(
                            SharedAxonServerContainer.INSTANCE.getHost(),
                            SharedAxonServerContainer.INSTANCE.getHttpPort(),
                            name,
                            AxonServerContainerUtils.DCB_CONTEXT,
                            DEFAULT_REPLICATION_GROUP
                    );
                } catch (IOException e) {
                    throw new RuntimeException("Failed to empty context in Axon Server", e);
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
         * Empty a context of all its events, leaving the context itself (and its configuration) in place.
         *
         * @param name the context name
         */
        void emptyContext(String name);

        /**
         * Delete the given contexts, ignoring any that don't currently exist (e.g. already deleted by the test
         * itself). The shared container may also be in use by other test classes at the same time, so this only
         * ever touches the exact names passed in -- never every custom context on the container.
         *
         * @param names the context names to delete
         */
        default void deleteContexts(String... names) {
            Set<String> own = Set.of(names);
            getContexts().stream()
                         .filter(own::contains)
                         .forEach(this::deleteContext);
        }
    }
}
