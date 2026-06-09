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
package io.axoniq.framework.messaging.multitenancy.command;

import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.NoSuchTenantException;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.command.TenantCommandSegmentFactory;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandHandler;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Implementation of a {@link CommandBus} that is aware of multiple tenant instances of a {@code CommandBus}. Each
 * {@code CommandBus} instance is considered a "tenant".
 * <p>
 * The {@code MultiTenantCommandBus} relies on a {@link TenantResolver} to dispatch commands via resolved tenant segment
 * of the {@code CommandBus}. {@link TenantCommandSegmentFactory} is as factory to create tenant segments with.
 *
 * @author Stefan Dragisic
 * @author Steven van Beelen
 * @author Jan Galinski
 * @since 4.6.0
 */
public class MultiTenantCommandBus implements CommandBus, MultiTenantAwareComponent {

    private final Map<QualifiedName, CommandHandler> handlers = new ConcurrentHashMap<>();
    private final Map<TenantDescriptor, CommandBus> tenantSegments = new ConcurrentHashMap<>();

    private final TenantCommandSegmentFactory tenantSegmentFactory;
    private final TenantResolver<Message> tenantResolver;

    /**
     * Instantiate a MultiTenantCommandBus.
     */
    public MultiTenantCommandBus(TenantCommandSegmentFactory tenantSegmentFactory,
                                 TenantResolver<Message> tenantResolver) {
        this.tenantSegmentFactory = tenantSegmentFactory;
        this.tenantResolver = tenantResolver;
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                            @Nullable ProcessingContext processingContext) {
        return resolveTenant(command).dispatch(command, processingContext);
    }

    @Override
    public CommandBus subscribe(QualifiedName name, CommandHandler commandHandler) {
        handlers.computeIfAbsent(name, k -> {
            tenantSegments.forEach((tenant, segment) -> segment.subscribe(name, commandHandler));
            return commandHandler;
        });
        return this;
    }


    @Override
    public Registration registerTenant(TenantDescriptor tenantDescriptor) {
        CommandBus tenantSegment = tenantSegmentFactory.apply(tenantDescriptor);
        tenantSegments.putIfAbsent(tenantDescriptor, tenantSegment);

        return unregisterTenantOnCancel(tenantDescriptor);
    }

    @Override
    public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
        tenantSegments.computeIfAbsent(tenantDescriptor, tenant -> {
            CommandBus tenantSegment = tenantSegmentFactory.apply(tenantDescriptor);

            handlers.forEach(tenantSegment::subscribe);

            return tenantSegment;
        });

        return unregisterTenantOnCancel(tenantDescriptor);
    }

    private CommandBus resolveTenant(CommandMessage commandMessage) {
        TenantDescriptor tenantDescriptor = tenantResolver.resolveTenant(commandMessage, tenantSegments.keySet());
        CommandBus tenantCommandBus = tenantSegments.get(tenantDescriptor);
        if (tenantCommandBus == null) {
            throw new NoSuchTenantException(tenantDescriptor.tenantId());
        }
        return tenantCommandBus;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenantSegments", tenantSegments);
    }

    private Registration unregisterTenantOnCancel(TenantDescriptor tenantDescriptor) {
        return () -> tenantSegments.remove(tenantDescriptor) != null;
    }
}
