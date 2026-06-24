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

package io.axoniq.framework.examples.cli;

import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolverRegistry;
import io.axoniq.framework.messaging.multitenancy.configuration.DefaultTenantResolverRegistry;
import org.axonframework.common.Registration;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.TypeReference;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class DescribeCommandTest {

    @Test
    void describeCommandPrintsTheWholeConfiguration() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        AxonConfiguration configuration = new TestConfiguration();

        new ReplConsole(configuration, new ByteArrayInputStream(new byte[0]), output)
                .commandLine()
                .execute("describe", "--to-file=false");

        assertThat(output.toString())
                .contains("Multi-tenant wiring")
                .contains("TenantProvider:")
                .contains("TenantResolverRegistry:")
                .contains("CommandBusConnector:")
                .contains("Describable components")
                .contains("<unnamed>")
                .contains("sample")
                .contains("description")
                .contains("broken = <left out due to error:");
    }

    @Test
    void describeCommandCanPrintANamedComponent() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        AxonConfiguration configuration = new TestConfiguration();

        new ReplConsole(configuration, new ByteArrayInputStream(new byte[0]), output)
                .commandLine()
                .execute("describe", "--component", "sample", "--to-file=false");

        assertThat(output.toString())
                .contains("description")
                .doesNotContain("No describable component");
    }

    @Test
    void describeCommandCanDisablePrettyPrinting() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        AxonConfiguration configuration = new TestConfiguration();

        new ReplConsole(configuration, new ByteArrayInputStream(new byte[0]), output)
                .commandLine()
                .execute("describe", "--component", "sample", "--pretty", "false", "--to-file=false");

        assertThat(output.toString())
                .doesNotContain("\n  \"description\"")
                .contains("\"description\":\"example component\"");
    }

    @Test
    void describeCommandWritesFullConfigurationToFile() throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        AxonConfiguration configuration = new TestConfiguration();
        Path describeFile = Files.createTempFile("describe-command", ".txt");
        String previousDescribeFile = System.getProperty("DESCRIBE_FILE");

        try {
            System.setProperty("DESCRIBE_FILE", describeFile.toString());

            new ReplConsole(configuration, new ByteArrayInputStream(new byte[0]), output)
                    .commandLine()
                    .execute("describe", "-f");

            assertThat(Files.readString(describeFile))
                    .contains("Multi-tenant wiring")
                    .contains("TenantProvider:")
                    .contains("CommandBusConnector:")
                    .contains("Describable components:");
        } finally {
            if (previousDescribeFile == null) {
                System.clearProperty("DESCRIBE_FILE");
            } else {
                System.setProperty("DESCRIBE_FILE", previousDescribeFile);
            }
            Files.deleteIfExists(describeFile);
        }
    }

    private static final class TestConfiguration implements AxonConfiguration {

        private final DescribableComponent sampleComponent =
                descriptor -> descriptor.describeProperty("description", "example component");
        private final DescribableComponent brokenComponent =
                descriptor -> { throw new IllegalStateException("boom"); };
        private final DescribableComponent unnamedComponent =
                descriptor -> descriptor.describeProperty("description", "unnamed component");
        private final TenantProvider tenantProvider = new TestTenantProvider();
        private final TenantResolverRegistry tenantResolverRegistry = new DefaultTenantResolverRegistry();
        private final CommandBusConnector commandBusConnector = new TestCommandBusConnector();

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("sample", sampleComponent);
            descriptor.describeProperty("broken", brokenComponent);
            descriptor.describeProperty(null, unnamedComponent);
        }

        @Override
        public <C> Optional<C> getOptionalComponent(Class<C> componentType, String name) {
            if (componentType.isAssignableFrom(TenantProvider.class) && name == null) {
                @SuppressWarnings("unchecked")
                C component = (C) tenantProvider;
                return Optional.of(component);
            }
            if (componentType.isAssignableFrom(TenantResolverRegistry.class) && name == null) {
                @SuppressWarnings("unchecked")
                C component = (C) tenantResolverRegistry;
                return Optional.of(component);
            }
            if (componentType.isAssignableFrom(CommandBusConnector.class) && name == null) {
                @SuppressWarnings("unchecked")
                C component = (C) commandBusConnector;
                return Optional.of(component);
            }
            if (componentType.isAssignableFrom(DescribableComponent.class) && "sample".equals(name)) {
                @SuppressWarnings("unchecked")
                C component = (C) sampleComponent;
                return Optional.of(component);
            }
            return Optional.empty();
        }

        @Override
        public <C> Optional<C> getOptionalComponent(TypeReference<C> typeReference, String name) {
            return Optional.empty();
        }

        @Override
        public <C> C getComponent(Class<C> componentType, String name, Supplier<C> supplier) {
            return getOptionalComponent(componentType, name).orElseGet(supplier);
        }

        @Override
        public List<Configuration> getModuleConfigurations() {
            return List.of();
        }

        @Override
        public Optional<Configuration> getModuleConfiguration(String name) {
            return Optional.empty();
        }

        @Override
        public Configuration getParent() {
            return null;
        }

        @Override
        public <C> Map<String, C> getComponents(Class<C> type) {
            if (type.isAssignableFrom(DescribableComponent.class)) {
                Map<String, C> components = new HashMap<>();
                @SuppressWarnings("unchecked")
                C sample = (C) sampleComponent;
                @SuppressWarnings("unchecked")
                C broken = (C) brokenComponent;
                @SuppressWarnings("unchecked")
                C unnamed = (C) unnamedComponent;
                components.put("sample", sample);
                components.put("broken", broken);
                components.put(null, unnamed);
                return components;
            }
            return Map.of();
        }

        @Override
        public void start() {
        }

        @Override
        public void shutdown() {
        }
    }

    private static final class TestTenantProvider implements TenantProvider, DescribableComponent {

        @Override
        public Registration subscribe(MultiTenantAwareComponent component) {
            return () -> true;
        }

        @Override
        public List<TenantDescriptor> getTenants() {
            return List.of(TenantDescriptor.tenantWithId("tenant-1"));
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("tenants", getTenants());
        }
    }

    private static final class TestCommandBusConnector implements CommandBusConnector {

        private final AtomicBoolean subscribed = new AtomicBoolean(false);

        @Override
        public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command, ProcessingContext processingContext) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
            subscribed.set(true);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean unsubscribe(QualifiedName commandName) {
            return subscribed.getAndSet(false);
        }

        @Override
        public void onIncomingCommand(Handler handler) {
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("subscribed", subscribed.get());
        }
    }
}
