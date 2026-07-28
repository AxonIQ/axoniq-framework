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
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolverRegistry;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.common.infra.JacksonComponentDescriptor;
import org.jspecify.annotations.Nullable;
import picocli.CommandLine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.Comparator;
import java.util.Map;

/**
 * Uses a {@link JacksonComponentDescriptor} to describe the components registered in the {@link CommandBusConnector}.
 * Used as a debugging support from the REPL while running the example application.
 */
@CommandLine.Command(name = "describe", description = "Describe the current configuration and its components.")
public final class DescribeCommand extends SubCommand {

    @CommandLine.Option(names = {"-c", "--component"},
                        description = "Optional component name to describe.")
    @Nullable
    private String componentName;

    @CommandLine.Option(names = {"-p", "--pretty"},
                        description = "Pretty-print JSON output. Defaults to true; pass false to disable.",
                        arity = "0..1",
                        defaultValue = "true")
    private boolean pretty = true;

    @Override
    public void run() {
        if (componentName == null || componentName.isBlank()) {
            describeConfiguration();
            return;
        }

        Map<String, DescribableComponent> components = repl.axonConfiguration.getComponents(DescribableComponent.class);
        DescribableComponent component = components.get(componentName);
        if (component == null) {
            echo("No describable component named [%s] was found.", componentName);
            return;
        }

        JacksonComponentDescriptor descriptor = new JacksonComponentDescriptor(objectMapper());
        component.describeTo(descriptor);
        echo(descriptor.describe());
    }

    private void describeConfiguration() {
        echo("Multi-tenant wiring:");
        describeWiringPoint("TenantProvider", TenantProvider.class);
        describeWiringPoint("TenantResolverRegistry", TenantResolverRegistry.class);
        describeWiringPoint("CommandBusConnector", CommandBusConnector.class);
        describeWiringPoint("CommandBus", org.axonframework.messaging.commandhandling.CommandBus.class);

        echo("Describable components:");
        Map<String, DescribableComponent> components = repl.axonConfiguration.getComponents(DescribableComponent.class);
        components.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey(Comparator.nullsLast(String::compareTo)))
                .forEach(entry -> describeComponentEntry(entry.getKey(), entry.getValue()));
    }

    private <T> void describeWiringPoint(String label, Class<T> type) {
        repl.axonConfiguration.getOptionalComponent(type)
                .ifPresentOrElse(
                        component -> echoComponent(label, component),
                        () -> echo("%s: <not registered>", label)
                );
    }

    private void describeComponentEntry(@Nullable String name, DescribableComponent component) {
        JacksonComponentDescriptor descriptor = new JacksonComponentDescriptor(objectMapper());
        try {
            component.describeTo(descriptor);
            echo("%s = %s", displayName(name), descriptor.describe());
        } catch (Exception e) {
            echo("%s = <left out due to error: %s>", displayName(name), rootCauseMessage(e));
        }
    }

    private void echoComponent(String label, Object component) {
        if (component instanceof DescribableComponent describable) {
            JacksonComponentDescriptor descriptor = new JacksonComponentDescriptor(objectMapper());
            try {
                describable.describeTo(descriptor);
                echo("%s:", label);
                echo(descriptor.describe());
            } catch (Exception e) {
                echo("%s: <left out due to error: %s>", label, rootCauseMessage(e));
            }
            return;
        }
        echo("%s: %s", label, component.getClass().getName());
    }

    private ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        if (pretty) {
            objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        }
        return objectMapper;
    }

    private static String displayName(@Nullable String name) {
        return name == null || name.isBlank() ? "<unnamed>" : name;
    }

    private static String rootCauseMessage(Throwable throwable) {
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
            rootCause = rootCause.getCause();
        }
        String message = rootCause.getMessage();
        return message == null || message.isBlank() ? rootCause.getClass().getName() : message;
    }

}
