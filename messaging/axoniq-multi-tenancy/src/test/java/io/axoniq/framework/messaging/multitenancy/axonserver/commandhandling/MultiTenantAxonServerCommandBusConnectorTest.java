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

package io.axoniq.framework.messaging.multitenancy.axonserver.commandhandling;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.admin.AdminChannel;
import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.connector.control.ControlChannel;
import io.axoniq.axonserver.connector.event.DcbEventChannel;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.SnapshotChannel;
import io.axoniq.axonserver.connector.event.transformation.EventTransformationChannel;
import io.axoniq.axonserver.connector.query.QueryChannel;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.command.Command;
import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.axonserver.connector.api.RecordingAxonServerConnectionManager;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentLookup;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.MockComponentDescriptor;
import org.axonframework.common.lifecycle.ShutdownInProgressException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MultiTenantAxonServerCommandBusConnectorTest {

    private static final TenantDescriptor TENANT_1 = TenantDescriptor.tenantWithId("tenant-1");
    private static final TenantDescriptor TENANT_2 = TenantDescriptor.tenantWithId("tenant-2");
    private static final QualifiedName COMMAND_ONE = new QualifiedName("command-one");
    private static final QualifiedName COMMAND_TWO = new QualifiedName("command-two");

    @Nested
    class ConstructorValidation {

        private final AxonServerConfiguration configuration = defaultConfiguration();
        private final AxonServerConnectionManager connectionManager =
                new RecordingAxonServerConnectionManager(configuration, Map.of());
        private final MessageConverter converter = Mockito.mock(MessageConverter.class);
        private final TenantRouter tenantRouter = new TenantRouter(
                new TenantResolver() {
                    @Override
                    public TenantDescriptor resolveTenant(Message message, Collection<TenantDescriptor> tenants) {
                        return tenants.stream()
                                      .findFirst()
                                      .orElseThrow(() -> new TenantNotResolvedException("no tenant"));
                    }

                    @Override
                    public Message attachTenant(Message message, TenantDescriptor tenant) {
                        return message.andMetadata(Map.of(TenantDescriptor.TENANT_ID_KEY, tenant.tenantId()));
                    }
                },
                List::of);

        @Test
        void rejectsNullTenantRouter() {
            assertThatThrownBy(() -> new MultiTenantAxonServerCommandBusConnector(
                    null, connectionManager, configuration, unused -> converter))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsNullConnectionManager() {
            assertThatThrownBy(() -> new MultiTenantAxonServerCommandBusConnector(
                    tenantRouter, null, configuration, unused -> converter))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsNullConfiguration() {
            assertThatThrownBy(() -> new MultiTenantAxonServerCommandBusConnector(
                    tenantRouter, connectionManager, null, unused -> converter))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsNullConverterLookup() {
            assertThatThrownBy(() -> new MultiTenantAxonServerCommandBusConnector(
                    tenantRouter, connectionManager, configuration, (TenantComponentLookup<MessageConverter>) null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class Start {

        @Test
        void startReopensDispatchingAfterShutdown() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));
            testSubject.shutdownDispatching().join();

            // when / then: dispatching while still shut down fails
            assertThatThrownBy(() -> testSubject.dispatch(commandFor(TENANT_1.tenantId()), null))
                    .isInstanceOf(ShutdownInProgressException.class);

            // when: the connector is started again
            testSubject.start();

            // then: dispatching is possible again
            assertThat(testSubject.dispatch(commandFor(TENANT_1.tenantId()), null)).isCompleted();
        }
    }

    @Nested
    class Dispatch {

        @Test
        void dispatchRoutesCommandToResolvedTenant() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            CommandMessage command = commandFor(TENANT_2.tenantId());
            CompletableFuture<CommandResultMessage> result = testSubject.dispatch(command, null);

            assertThat(result).isCompleted();
            assertThat(connection1.recordingCommandChannel().sentCommands()).isEmpty();
            assertThat(connection2.recordingCommandChannel().sentCommands()).hasSize(1);
            assertThat(connection2.recordingCommandChannel().sentCommands().get(0).getMessageIdentifier())
                    .isEqualTo(command.identifier());
        }

        @Test
        void dispatchRoutesCommandWhenResolvedTenantHasOnlyAnId() {
            TenantDescriptor tenantWithProperties = new TenantDescriptor(
                    TENANT_2.tenantId(),
                    Map.of("replicationGroup", "rg-2")
            );
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, tenantWithProperties));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            CommandMessage command = commandFor(TENANT_2.tenantId());
            CompletableFuture<CommandResultMessage> result = testSubject.dispatch(command, null);

            assertThat(result).isCompleted();
            assertThat(connection2.recordingCommandChannel().sentCommands()).hasSize(1);
        }

        @Test
        void dispatchRoutesOnTheTenantCarriedByTheProcessingContext() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            CommandMessage command = commandWithoutTenant();
            ProcessingContext context = StubProcessingContext.forMessage(command)
                                                             .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_2);
            CompletableFuture<CommandResultMessage> result = testSubject.dispatch(command, context);

            assertThat(result).isCompleted();
            assertThat(connection1.recordingCommandChannel().sentCommands()).isEmpty();
            assertThat(connection2.recordingCommandChannel().sentCommands()).hasSize(1);
        }

        @Test
        void dispatchPrefersTheTenantOfTheProcessingContextOverTheTenantNamedInTheCommand() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            // Command metadata must not redirect a command out of the tenant it is dispatched in.
            CommandMessage command = commandFor(TENANT_1.tenantId());
            ProcessingContext context = StubProcessingContext.forMessage(command)
                                                             .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_2);
            CompletableFuture<CommandResultMessage> result = testSubject.dispatch(command, context);

            assertThat(result).isCompleted();
            assertThat(connection1.recordingCommandChannel().sentCommands()).isEmpty();
            assertThat(connection2.recordingCommandChannel().sentCommands()).hasSize(1);
        }

        @Test
        void dispatchingWithAContextNamingAnUnknownTenantFailsRatherThanFallingBackToTheCommand() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));

            CommandMessage command = commandFor(TENANT_1.tenantId());
            ProcessingContext context = StubProcessingContext.forMessage(command)
                                                             .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_2);

            assertThatThrownBy(() -> testSubject.dispatch(command, context))
                    .isInstanceOf(TenantNotResolvedException.class);
            assertThat(connection1.recordingCommandChannel().sentCommands()).isEmpty();
        }

        @Test
        void dispatchingWithUnknownTenantFails() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));

            assertThatThrownBy(() -> testSubject.dispatch(commandFor(TENANT_2.tenantId()), null))
                    .isInstanceOf(TenantNotResolvedException.class);
        }
    }

    @Nested
    class Subscribe {

        @Test
        void subscribeRegistersCommandHandlersOnAllKnownTenants() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            testSubject.subscribe(COMMAND_ONE, 100).join();
            testSubject.subscribe(COMMAND_TWO, 50).join();

            assertThat(connection1.recordingCommandChannel().registeredCommandNames())
                    .containsExactlyInAnyOrder(COMMAND_ONE.name(), COMMAND_TWO.name());
            assertThat(connection2.recordingCommandChannel().registeredCommandNames())
                    .containsExactlyInAnyOrder(COMMAND_ONE.name(), COMMAND_TWO.name());
        }

        @Test
        void resubscribingSameCommandIsANoOpAndDoesNotCreateNewRegistration() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));

            // when
            testSubject.subscribe(COMMAND_ONE, 100).join();
            testSubject.subscribe(COMMAND_ONE, 999).join();

            // then
            List<RecordingRegistration> registrationsForCommand =
                    connection1.recordingCommandChannel().allRegistrationsFor(COMMAND_ONE.name());
            assertThat(registrationsForCommand).hasSize(1);
        }

        @Test
        void subscribingWithNoTenantsCompletesImmediately() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of());
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider, Map.of());

            // when
            CompletableFuture<Void> result = testSubject.subscribe(COMMAND_ONE, 100);

            // then
            assertThat(result).isCompleted();
        }
    }

    @Nested
    class Unsubscribe {

        @Test
        void unsubscribeRemovesCommandFromAllTenants() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            testSubject.subscribe(COMMAND_ONE, 100).join();
            boolean unsubscribed = testSubject.unsubscribe(COMMAND_ONE);

            assertThat(unsubscribed).isTrue();
            assertThat(connection1.recordingCommandChannel().registeredCommandNames())
                    .doesNotContain(COMMAND_ONE.name());
            assertThat(connection2.recordingCommandChannel().registeredCommandNames())
                    .doesNotContain(COMMAND_ONE.name());
        }

        @Test
        void unsubscribingUnknownCommandReturnsFalse() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));

            // when
            boolean result = testSubject.unsubscribe(COMMAND_ONE);

            // then
            assertThat(result).isFalse();
        }
    }

    @Nested
    class OnIncomingCommand {

        @Test
        void onIncomingCommandIsPropagatedToTenantsRegisteredAfterHandlerIsSet() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                 Map.of(TENANT_1.tenantId(),
                                                                                        connection1,
                                                                                        TENANT_2.tenantId(),
                                                                                        connection2));

            testSubject.subscribe(COMMAND_ONE, 100).join();

            AtomicReference<CommandBusConnector.ResultCallback> resultCallback = new AtomicReference<>();
            testSubject.onIncomingCommand((commandMessage, callback) -> resultCallback.set(callback));

            // TENANT_2 registers after both subscribe() and onIncomingCommand() were already called
            tenantProvider.addTenant(TENANT_2);

            Command incomingCommand = Command.newBuilder()
                                             .setMessageIdentifier("incoming-message-id")
                                             .setName(COMMAND_ONE.name())
                                             .setPayload(SerializedObject.newBuilder()
                                                                         .setType(COMMAND_ONE.name())
                                                                         .setRevision("0")
                                                                         .setData(ByteString.EMPTY)
                                                                         .build())
                                             .build();
            connection2.recordingCommandChannel().simulateIncomingCommand(incomingCommand);

            assertThat(resultCallback.get())
                    .as("Command on the tenant registered after onIncomingCommand() was not received")
                    .isNotNull();
        }

        @Test
        void onIncomingCommandReachesTenantsRegisteredBeforeHandlerIsSet() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));
            testSubject.subscribe(COMMAND_ONE, 100).join();

            // when
            AtomicReference<CommandBusConnector.ResultCallback> resultCallback = new AtomicReference<>();
            testSubject.onIncomingCommand((commandMessage, callback) -> resultCallback.set(callback));

            Command incomingCommand = Command.newBuilder()
                                             .setMessageIdentifier("incoming-message-id")
                                             .setName(COMMAND_ONE.name())
                                             .setPayload(SerializedObject.newBuilder()
                                                                         .setType(COMMAND_ONE.name())
                                                                         .setRevision("0")
                                                                         .setData(ByteString.EMPTY)
                                                                         .build())
                                             .build();
            connection1.recordingCommandChannel().simulateIncomingCommand(incomingCommand);

            // then
            assertThat(resultCallback.get())
                    .as("Command on the tenant registered before onIncomingCommand() was not received")
                    .isNotNull();
        }

        @Test
        void onIncomingCommandReplacesPreviouslySetHandler() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));
            testSubject.subscribe(COMMAND_ONE, 100).join();

            AtomicReference<CommandBusConnector.ResultCallback> firstHandlerCallback = new AtomicReference<>();
            testSubject.onIncomingCommand((commandMessage, callback) -> firstHandlerCallback.set(callback));

            // when
            AtomicReference<CommandBusConnector.ResultCallback> secondHandlerCallback = new AtomicReference<>();
            testSubject.onIncomingCommand((commandMessage, callback) -> secondHandlerCallback.set(callback));

            Command incomingCommand = Command.newBuilder()
                                             .setMessageIdentifier("incoming-message-id")
                                             .setName(COMMAND_ONE.name())
                                             .setPayload(SerializedObject.newBuilder()
                                                                         .setType(COMMAND_ONE.name())
                                                                         .setRevision("0")
                                                                         .setData(ByteString.EMPTY)
                                                                         .build())
                                             .build();
            connection1.recordingCommandChannel().simulateIncomingCommand(incomingCommand);

            // then
            assertThat(secondHandlerCallback.get())
                    .as("The replacing handler should receive the command")
                    .isNotNull();
            assertThat(firstHandlerCallback.get())
                    .as("The replaced handler should no longer receive commands")
                    .isNull();
        }
    }

    @Nested
    class ShutdownDispatching {

        @Test
        void shutdownDispatchingCompletesForAllTenantConnectors() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            // when
            CompletableFuture<Void> result = testSubject.shutdownDispatching();

            // then
            assertThat(result).isCompleted();
        }

        @Test
        void shutdownDispatchingWithNoTenantsCompletesImmediately() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of());
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider, Map.of());

            // when
            CompletableFuture<Void> result = testSubject.shutdownDispatching();

            // then
            assertThat(result).isCompleted();
        }
    }

    @Nested
    class Disconnect {

        @Test
        void disconnectPreparesAllTenantCommandChannelsWithoutClosingConnections() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            testSubject.subscribe(COMMAND_ONE, 100).join();

            testSubject.disconnect().join();

            assertThat(connection1.recordingCommandChannel().prepareDisconnectCalled()).isTrue();
            assertThat(connection2.recordingCommandChannel().prepareDisconnectCalled()).isTrue();
            assertThat(connection1.disconnectCalls()).isZero();
            assertThat(connection2.disconnectCalls()).isZero();
        }

    }

    @Nested
    class TenantRegistration {

        @Test
        void registerAndStartTenantReplaysKnownCommands() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));

            testSubject.subscribe(COMMAND_ONE, 100).join();

            tenantProvider.addTenant(TENANT_2);

            assertThat(connection2.recordingCommandChannel().registeredCommandNames())
                    .contains(COMMAND_ONE.name());
        }

        @Test
        void duplicateTenantRegistrationDoesNotLeakOrphanedCommandRegistration() {
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));

            testSubject.subscribe(COMMAND_ONE, 100).join();

            // TENANT_1 already has a connector; re-registering it must not replay known subscriptions onto that
            // already-in-sync connector, since AxonServerCommandBusConnector#subscribe(QualifiedName, int) does not
            // cancel a prior registration for the same command name and would otherwise leak one.
            testSubject.registerTenant(TENANT_1);

            List<RecordingRegistration> registrationsForCommand =
                    connection1.recordingCommandChannel().allRegistrationsFor(COMMAND_ONE.name());
            long activeRegistrations = registrationsForCommand.stream()
                                                              .filter(registration -> !registration.isCancelled())
                                                              .count();

            assertThat(activeRegistrations)
                    .as("Re-registering an already-known tenant must not leave an orphaned, uncancelled command "
                                + "registration behind")
                    .isEqualTo(1);
        }
    }

    @Nested
    class TenantRemoval {

        @Test
        void cancellingRegistrationRemovesTenantAndPreparesItsConnectorForDisconnect() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2));
            Registration registration = testSubject.registerTenant(TENANT_1);

            // when
            boolean cancelled = registration.cancel();

            // then
            assertThat(cancelled).isTrue();
            assertThat(connection1.recordingCommandChannel().prepareDisconnectCalled()).isTrue();
            assertThat(connection1.disconnectCalls()).isZero();
            assertThatThrownBy(() -> testSubject.dispatch(commandFor(TENANT_1.tenantId()), null))
                    .isInstanceOf(TenantNotResolvedException.class);
            assertThat(testSubject.dispatch(commandFor(TENANT_2.tenantId()), null)).isCompleted();
        }

        @Test
        void cancellingAlreadyRemovedTenantRegistrationReturnsFalse() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1));
            RecordingConnection connection1 = new RecordingConnection();
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1));
            Registration registration = testSubject.registerTenant(TENANT_1);
            registration.cancel();

            // when
            boolean secondCancel = registration.cancel();

            // then
            assertThat(secondCancel).isFalse();
        }
    }

    @Nested
    class DescribeTo {

        @Test
        void describeToExposesTheRouterSubscriptionsAndConnectors() {
            // given
            TestTenantProvider tenantProvider = new TestTenantProvider(List.of(TENANT_1, TENANT_2));
            RecordingConnection connection1 = new RecordingConnection();
            RecordingConnection connection2 = new RecordingConnection();
            TenantRouter tenantRouter = routerFor(tenantProvider);
            MultiTenantAxonServerCommandBusConnector testSubject = createSubject(tenantProvider,
                                                                                Map.of(TENANT_1.tenantId(),
                                                                                       connection1,
                                                                                       TENANT_2.tenantId(),
                                                                                       connection2),
                                                                                tenantRouter);
            testSubject.subscribe(COMMAND_ONE, 100).join();

            // when
            MockComponentDescriptor descriptor = new MockComponentDescriptor();
            testSubject.describeTo(descriptor);

            // then
            assertThat((TenantRouter) descriptor.getProperty("tenantRouter")).isSameAs(tenantRouter);

            Map<QualifiedName, Integer> describedSubscriptions = descriptor.getProperty("subscribedCommands");
            assertThat(describedSubscriptions).containsEntry(COMMAND_ONE, 100);

            Map<String, ?> describedConnectors = descriptor.getProperty("tenantConnectors");
            assertThat(describedConnectors).containsOnlyKeys(TENANT_1.tenantId(), TENANT_2.tenantId());
        }
    }

    private static AxonServerConfiguration defaultConfiguration() {
        AxonServerConfiguration configuration = new AxonServerConfiguration();
        configuration.setClientId("client-id");
        configuration.setComponentName("component-name");
        return configuration;
    }

    private static MultiTenantAxonServerCommandBusConnector createSubject(TestTenantProvider tenantProvider,
                                                                           Map<String, RecordingConnection> connections) {
        return createSubject(tenantProvider, connections, routerFor(tenantProvider));
    }

    private static MultiTenantAxonServerCommandBusConnector createSubject(TestTenantProvider tenantProvider,
                                                                           Map<String, RecordingConnection> connections,
                                                                           TenantRouter tenantRouter) {
        AxonServerConfiguration configuration = defaultConfiguration();
        MessageConverter converter = Mockito.mock(MessageConverter.class);
        MultiTenantAxonServerCommandBusConnector connector = new MultiTenantAxonServerCommandBusConnector(
                tenantRouter,
                new RecordingAxonServerConnectionManager(configuration, connections),
                configuration,
                unused -> converter
        );
        tenantProvider.subscribe(connector);
        return connector;
    }

    /**
     * A router resolving on the tenant named in the command's metadata, against the tenants the provider knows.
     */
    private static TenantRouter routerFor(TestTenantProvider tenantProvider) {
        return new TenantRouter(
                new TenantResolver() {
                    @Override
                    public TenantDescriptor resolveTenant(Message message, Collection<TenantDescriptor> tenants) {
                        return tenants.stream()
                                      .filter(tenant -> tenant.tenantId().equals(message.metadata().get("tenantId")))
                                      .findFirst()
                                      .orElseThrow(() -> new TenantNotResolvedException(
                                              "No tenant found in metadata"));
                    }

                    @Override
                    public Message attachTenant(Message message, TenantDescriptor tenant) {
                        return message.andMetadata(Map.of("tenantId", tenant.tenantId()));
                    }
                },
                tenantProvider);
    }

    private static CommandMessage commandWithoutTenant() {
        return new GenericCommandMessage(
                new GenericMessage("message-id", new MessageType(COMMAND_ONE.name()), "payload".getBytes(), Map.of())
        );
    }

    private static CommandMessage commandFor(String tenantId) {
        return new GenericCommandMessage(
                new GenericMessage("message-id",
                                   new MessageType(COMMAND_ONE.name()),
                                   "payload".getBytes(),
                                   Map.of("tenantId", tenantId))
        );
    }

    private static final class TestTenantProvider implements TenantProvider {

        private final List<TenantDescriptor> tenants = new ArrayList<>();
        private final List<io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent> components =
                new ArrayList<>();

        private TestTenantProvider(Collection<TenantDescriptor> tenants) {
            this.tenants.addAll(tenants);
        }

        @Override
        public Registration subscribe(
                io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent component) {
            components.add(component);
            tenants.forEach(component::registerAndStartTenant);
            return () -> components.remove(component);
        }

        @Override
        public List<TenantDescriptor> tenants() {
            return List.copyOf(tenants);
        }

        private void addTenant(TenantDescriptor tenantDescriptor) {
            tenants.add(tenantDescriptor);
            components.forEach(component -> component.registerAndStartTenant(tenantDescriptor));
        }
    }

    private static final class RecordingConnection implements AxonServerConnection {

        private final RecordingCommandChannel commandChannel = new RecordingCommandChannel();
        private int disconnectCalls;
        private boolean connected = true;

        @Override
        public boolean isConnectionFailed() {
            return false;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public boolean isConnected() {
            return connected;
        }

        @Override
        public void disconnect() {
            connected = false;
            disconnectCalls++;
        }

        @Override
        public ControlChannel controlChannel() {
            return null;
        }

        @Override
        public CommandChannel commandChannel() {
            return commandChannel;
        }

        @Override
        public EventChannel eventChannel() {
            return null;
        }

        @Override
        public DcbEventChannel dcbEventChannel() {
            return null;
        }

        @Override
        public QueryChannel queryChannel() {
            return null;
        }

        @Override
        public SnapshotChannel snapshotChannel() {
            return null;
        }

        @Override
        public EventTransformationChannel eventTransformationChannel() {
            return null;
        }

        @Override
        public AdminChannel adminChannel() {
            return null;
        }

        private RecordingCommandChannel recordingCommandChannel() {
            return commandChannel;
        }

        private int disconnectCalls() {
            return disconnectCalls;
        }
    }

    private static final class RecordingCommandChannel implements CommandChannel {

        private final List<Command> sentCommands = new ArrayList<>();
        private final Map<String, RecordingRegistration> registrations = new LinkedHashMap<>();
        private final Map<String, List<RecordingRegistration>> allRegistrationsByCommand = new LinkedHashMap<>();
        private final Map<String, java.util.function.Function<Command, CompletableFuture<CommandResponse>>> handlers =
                new LinkedHashMap<>();
        private boolean prepareDisconnectCalled;

        @Override
        public io.axoniq.axonserver.connector.Registration registerCommandHandler(
                java.util.function.Function<Command, CompletableFuture<CommandResponse>> handler,
                int loadFactor,
                String... commandNames) {
            RecordingRegistration registration = new RecordingRegistration(registrations, commandNames);
            for (String commandName : commandNames) {
                registrations.put(commandName, registration);
                handlers.put(commandName, handler);
                allRegistrationsByCommand.computeIfAbsent(commandName, key -> new ArrayList<>()).add(registration);
            }
            return registration;
        }

        private List<RecordingRegistration> allRegistrationsFor(String commandName) {
            return List.copyOf(allRegistrationsByCommand.getOrDefault(commandName, List.of()));
        }

        private CompletableFuture<CommandResponse> simulateIncomingCommand(Command command) {
            return handlers.get(command.getName()).apply(command);
        }

        @Override
        public CompletableFuture<CommandResponse> sendCommand(Command command) {
            sentCommands.add(command);
            return CompletableFuture.completedFuture(CommandResponse.newBuilder()
                                                                   .setMessageIdentifier(command.getMessageIdentifier())
                                                                   .setPayload(SerializedObject.newBuilder()
                                                                                               .setType("")
                                                                                               .setRevision("")
                                                                                               .setData(ByteString.EMPTY)
                                                                                               .build())
                                                                   .build());
        }

        @Override
        public CompletableFuture<Void> prepareDisconnect() {
            prepareDisconnectCalled = true;
            return CompletableFuture.completedFuture(null);
        }

        private List<Command> sentCommands() {
            return sentCommands;
        }

        private List<String> registeredCommandNames() {
            return List.copyOf(registrations.keySet());
        }

        private boolean prepareDisconnectCalled() {
            return prepareDisconnectCalled;
        }
    }

    private static final class RecordingRegistration implements io.axoniq.axonserver.connector.Registration {

        private final Map<String, RecordingRegistration> registrations;
        private final List<String> commandNames;
        private boolean cancelled;

        private RecordingRegistration(Map<String, RecordingRegistration> registrations, String... commandNames) {
            this.registrations = registrations;
            this.commandNames = List.of(commandNames);
        }

        @Override
        public CompletableFuture<Void> cancel() {
            cancelled = true;
            commandNames.forEach(registrations::remove);
            return CompletableFuture.completedFuture(null);
        }

        private boolean isCancelled() {
            return cancelled;
        }
    }
}
