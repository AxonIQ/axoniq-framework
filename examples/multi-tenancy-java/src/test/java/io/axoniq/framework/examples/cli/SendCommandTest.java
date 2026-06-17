/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
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

import org.axonframework.common.TypeReference;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.commandhandling.gateway.CommandResult;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class SendCommandTest {

    @Test
    void commandWithoutResultMessageCompletesSuccessfully() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        AxonConfiguration configuration = new TestConfiguration();

        assertThatNoException().isThrownBy(() -> new ReplConsole(configuration, new ByteArrayInputStream(new byte[0]), output)
                .commandLine()
                .execute("command", "-t", "B", "-c", "CREATE_COURSE", "-p", "course-1,Foo,1"));

        assertThat(output.toString())
                .contains("Published command")
                .contains("foo-b")
                .doesNotContain("->");
    }

    private static final class TestConfiguration implements AxonConfiguration {

        private final CommandGateway commandGateway = new TestCommandGateway();

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
        }

        @Override
        public <C> Optional<C> getOptionalComponent(Class<C> componentType, String name) {
            if (componentType.isAssignableFrom(CommandGateway.class) && name == null) {
                @SuppressWarnings("unchecked")
                C component = (C) commandGateway;
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
            if (type.isAssignableFrom(CommandGateway.class)) {
                return Map.of();
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

    private static final class TestCommandGateway implements CommandGateway {

        @Override
        public CommandResult send(Object command, Metadata metadata, ProcessingContext context) {
            return () -> CompletableFuture.completedFuture(null);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeProperty("gateway", "null-result");
        }
    }
}
