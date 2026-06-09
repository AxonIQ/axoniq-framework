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
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.messaging.multitenancy.command;

import io.axoniq.framework.messaging.multitenancy.api.NoSuchTenantException;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.command.TenantCommandSegmentFactory;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandHandler;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MultiTenantCommandBusTest {

    private static final TenantDescriptor TENANT = TenantDescriptor.tenantWithId("tenant-1");
    private static final TenantDescriptor UNKNOWN_TENANT = TenantDescriptor.tenantWithId("tenant-unknown");
    private static final QualifiedName COMMAND_ONE = new QualifiedName("command-one");
    private static final QualifiedName COMMAND_TWO = new QualifiedName("command-two");

    @Nested
    class GivenTenantIsRegisteredFirst {

        @Test
        void subscribeStoresAllTenantRegistrationsAndCancelsThemOnUnregister() {
            RecordingCommandBus tenantBus = new RecordingCommandBus();
            MultiTenantCommandBus testSubject = createSubject(tenantBus);

            // given
            Registration tenantRegistration = testSubject.registerTenant(TENANT);

            CommandHandler handlerOne = (command, context) -> MessageStream.empty();
            CommandHandler handlerTwo = (command, context) -> MessageStream.empty();

            // when
            testSubject.subscribe(COMMAND_ONE, handlerOne);
            testSubject.subscribe(COMMAND_TWO, handlerTwo);

            // then
            assertThat(tenantBus.subscriptions()).containsKeys(COMMAND_ONE, COMMAND_TWO);

            // when
            boolean cancelled = tenantRegistration.cancel();

            // then
            assertThat(cancelled).isTrue();
            assertThatThrownBy(() -> testSubject.dispatch(commandFor(COMMAND_ONE), null))
                    .isInstanceOf(NoSuchTenantException.class);
        }
    }

    @Nested
    class GivenHandlersAreRegisteredFirst {

        @Test
        void registerAndStartTenantSubscribesExistingHandlersAndCancelsThemOnUnregister() {
            RecordingCommandBus tenantBus = new RecordingCommandBus();
            MultiTenantCommandBus testSubject = createSubject(tenantBus);

            // given
            CommandHandler handlerOne = (command, context) -> MessageStream.empty();
            CommandHandler handlerTwo = (command, context) -> MessageStream.empty();
            testSubject.subscribe(COMMAND_ONE, handlerOne);
            testSubject.subscribe(COMMAND_TWO, handlerTwo);

            // when
            Registration tenantRegistration = testSubject.registerAndStartTenant(TENANT);

            // then
            assertThat(tenantBus.subscriptions()).containsKeys(COMMAND_ONE, COMMAND_TWO);

            // when
            boolean cancelled = tenantRegistration.cancel();

            // then
            assertThat(cancelled).isTrue();
            assertThatThrownBy(() -> testSubject.dispatch(commandFor(COMMAND_ONE), null))
                    .isInstanceOf(NoSuchTenantException.class);
        }
    }

    @Test
    void dispatchingWithUnknownTenantFails() {
        MultiTenantCommandBus testSubject = createSubject(new RecordingCommandBus());

        // given
        testSubject.registerTenant(TENANT);

        // when / then
        assertThatThrownBy(() -> testSubject.dispatch(commandFor(COMMAND_ONE), null))
                .isInstanceOf(NoSuchTenantException.class)
                .hasMessage("Tenant with identifier [tenant-unknown] is unknown");
    }

    private static MultiTenantCommandBus createSubject(RecordingCommandBus tenantBus) {
        TenantCommandSegmentFactory tenantCommandSegmentFactory = tenant -> tenantBus;
        TenantResolver<Message> tenantResolver = (message, tenants) -> UNKNOWN_TENANT;
        return new MultiTenantCommandBus(tenantCommandSegmentFactory, tenantResolver);
    }

    private static CommandMessage commandFor(QualifiedName name) {
        return new GenericCommandMessage(new MessageType(name.name()), "payload");
    }

    private static final class RecordingCommandBus implements CommandBus {

        private final Map<QualifiedName, Boolean> subscriptions = new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                                ProcessingContext processingContext) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CommandBus subscribe(QualifiedName name, CommandHandler commandHandler) {
            subscriptions.put(name, Boolean.TRUE);
            return this;
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            // no-op
        }

        public Map<QualifiedName, Boolean> subscriptions() {
            return subscriptions;
        }
    }

}
