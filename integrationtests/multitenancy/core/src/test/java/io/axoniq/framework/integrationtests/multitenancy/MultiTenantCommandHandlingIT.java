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

package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.multitenancy.annotation.TenantScoped;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.DefaultAxonApplication;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandDispatcher;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.commandhandling.gateway.CommandResult;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException.tenantNotResolved;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for the multi-tenancy feature testing against multi-context Axon Server.
 * <p>
 * The application and its tenant contexts are built once for the whole class rather than per test, to avoid paying
 * for a full application startup and context round-trip per test method.
 *
 * @author Jan Galinski
 * @author Jakob Hatzl
 * @since 5.3.0
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultiTenantCommandHandlingIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String TENANT_A = "command-tenant-a";
    private static final String TENANT_B = "command-tenant-b";
    // Dedicated to the one test that deletes a tenant, so that destructive test doesn't affect TENANT_A/TENANT_B,
    // which every other test in this class relies on remaining present regardless of test execution order.
    private static final String TENANT_TO_DELETE = "command-tenant-to-delete";

    private AxonServerTestInfrastructure.ContextManager contextManager;
    private AxonConfiguration application;
    private final Queue<RecordedCommand> recordedCommands = new ConcurrentLinkedQueue<>();
    private final Queue<RecordedCommand> resolvedTenantScopedComponents = new ConcurrentLinkedQueue<>();
    private TenantProvider tenantDescriptors;

    @BeforeAll
    void setUpClass() {
        INFRASTRUCTURE.start();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.createContext(TENANT_A);
        contextManager.createContext(TENANT_B);
        contextManager.createContext(TENANT_TO_DELETE);
        assertThat(contextManager.getContexts())
                .containsExactlyInAnyOrder(ADMIN_CONTEXT, DEFAULT_CONTEXT, TENANT_A, TENANT_B, TENANT_TO_DELETE);

        CommandHandlingModule.CommandHandlerPhase commandHandlingModule =
                CommandHandlingModule.named("multi-tenancy-it-module")
                                     .commandHandlers()
                                     .commandHandler(new QualifiedName(RecordTenantCommand.class),
                                                     this::recordAndAcknowledge)
                                     .autodetectedCommandHandlingComponent(cfg -> this);

        application = new DefaultAxonApplication()
                .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                .componentRegistry(TenantFixture::connectOnlyCustomTenantsPredicate)

                // Identity factory: the tenant-scoped component IS the resolved TenantDescriptor, so injecting it
                // into the annotated handler below proves parameter resolution picks the dispatched tenant's instance.
                .componentRegistry(registry -> registry.registerComponent(TenantComponentProvider.class,
                                                                          cfg -> TenantComponentProvider.withFactory(
                                                                                  TenantDescriptor.class,
                                                                                  tenant -> tenant)))
                .componentRegistry(cr -> cr.registerModule(commandHandlingModule.build()))
                .start();

        tenantDescriptors = application.getComponent(TenantProvider.class);
    }

    @AfterAll
    void tearDownClass() {
        // Guarded: if setUpClass() failed after creating the tenant contexts but before application was assigned,
        // application.shutdown() would NPE and skip cleanup below, orphaning the contexts on the shared container.
        if (application != null) {
            application.shutdown();
        }
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    @BeforeEach
    void setUp() {
        recordedCommands.clear();
        resolvedTenantScopedComponents.clear();
    }

    @Test
    void commandSentViaTenantContextWithTenantMeta() {
        CommandGateway commandGateway = application.getComponent(CommandGateway.class);

        commandGateway.send(new RecordTenantCommand("for-tenant-a"),
                            Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, TENANT_A),
                            null);
        commandGateway.send(new RecordTenantCommand("for-tenant-b"),
                            Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, TENANT_B),
                            null);

        await().untilAsserted(() -> assertThat(recordedCommands).hasSize(2));

        assertThat(recordedCommands)
                .filteredOn(recorded -> recorded.commandId().equals("for-tenant-a"))
                .extracting(RecordedCommand::tenantId)
                .containsExactly(TENANT_A);
        assertThat(recordedCommands)
                .filteredOn(recorded -> recorded.commandId().equals("for-tenant-b"))
                .extracting(RecordedCommand::tenantId)
                .containsExactly(TENANT_B);
    }

    @Test
    void commandSentViaDynamicallyAddedTenant() {
        String dynamicTenant = "tenant-D";
        CommandGateway commandGateway = application.getComponent(CommandGateway.class);

        commandGateway.send(new RecordTenantCommand("for-tenant-a"),
                            Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, TENANT_A),
                            null);

        contextManager.createContext(dynamicTenant);

        await().untilAsserted(() -> assertThat(tenantDescriptors.tenants()).anyMatch(d -> dynamicTenant.equals(d.tenantId())));

        commandGateway.send(new RecordTenantCommand("for-tenant-d"),
                            Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, dynamicTenant),
                            null);

        await().untilAsserted(() -> assertThat(recordedCommands).hasSize(2));

        assertThat(recordedCommands)
                .filteredOn(recorded -> recorded.commandId().equals("for-tenant-a"))
                .extracting(RecordedCommand::tenantId)
                .containsExactly(TENANT_A);
        assertThat(recordedCommands)
                .filteredOn(recorded -> recorded.commandId().equals("for-tenant-d"))
                .extracting(RecordedCommand::tenantId)
                .containsExactly(dynamicTenant);
    }

    @Test
    void sendingCommandToDeletedTenantFails() {
        CommandGateway commandGateway = application.getComponent(CommandGateway.class);
        contextManager.deleteContext(TENANT_TO_DELETE);
        await().untilAsserted(() -> assertThat(tenantDescriptors.tenants())
                .noneMatch(d -> TENANT_TO_DELETE.equals(d.tenantId())));

        CommandResult result = commandGateway.send(new RecordTenantCommand("for-deleted-tenant"),
                                                   Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY,
                                                                 TENANT_TO_DELETE),
                                                   null);
        assertThatThrownBy(() -> result.getResultMessage().join())
                .hasCauseInstanceOf(TenantNotResolvedException.class);


        assertThat(recordedCommands).hasSize(0);
    }

    @Test
    void commandHandlerReceivesTenantScopedComponentMatchingTheDispatchedTenant() {
        CommandGateway commandGateway = application.getComponent(CommandGateway.class);

        commandGateway.send(new ResolveTenantScopedComponentCommand("for-tenant-a"),
                            Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, TENANT_A),
                            null);
        commandGateway.send(new ResolveTenantScopedComponentCommand("for-tenant-b"),
                            Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, TENANT_B),
                            null);

        await().untilAsserted(() -> assertThat(resolvedTenantScopedComponents).hasSize(2));

        assertThat(resolvedTenantScopedComponents)
                .filteredOn(recorded -> recorded.commandId().equals("for-tenant-a"))
                .extracting(RecordedCommand::tenantId)
                .containsExactly(TENANT_A);
        assertThat(resolvedTenantScopedComponents)
                .filteredOn(recorded -> recorded.commandId().equals("for-tenant-b"))
                .extracting(RecordedCommand::tenantId)
                .containsExactly(TENANT_B);
    }

    @Test
    void followUpCommandDispatchedFromWithinAHandlerStaysWithTheTenantOfTheHandledMessage() {
        CommandGateway commandGateway = application.getComponent(CommandGateway.class);

        commandGateway.send(new DispatchFollowUpCommand("chained"),
                            Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, TENANT_A),
                            null);

        // then the follow-up RecordTenantCommand, dispatched without naming a tenant, is recorded under TENANT_A
        await().untilAsserted(() -> assertThat(recordedCommands).hasSize(1));
        assertThat(recordedCommands).extracting(RecordedCommand::tenantId).containsExactly(TENANT_A);
    }

    @CommandHandler
    String resolveTenantScopedComponent(ResolveTenantScopedComponentCommand command,
                                       @TenantScoped TenantDescriptor tenantScopedComponent) {
        resolvedTenantScopedComponents.add(new RecordedCommand(command.id(), tenantScopedComponent.tenantId()));
        return "ok";
    }

    @CommandHandler
    void dispatchFollowUp(DispatchFollowUpCommand command, CommandDispatcher dispatcher) {
        dispatcher.send(new RecordTenantCommand(command.id()));
    }

    private MessageStream.Single<CommandResultMessage> recordAndAcknowledge(
            CommandMessage command,
            ProcessingContext context
    ) {
        RecordTenantCommand payload = command.payloadAs(RecordTenantCommand.class);
        String tenantId = TenantDescriptor
                .fromContext(context)
                .orElseThrow(tenantNotResolved("No tenant descriptor found in processing context"))
                .tenantId();
        recordedCommands.add(new RecordedCommand(payload.id(), tenantId));
        return MessageStream.just(new GenericCommandResultMessage(new MessageType(String.class), "ok"));
    }

    public record RecordTenantCommand(String id) {

    }

    public record ResolveTenantScopedComponentCommand(String id) {

    }

    public record DispatchFollowUpCommand(String id) {

    }

    public record RecordedCommand(String commandId, String tenantId) {

    }
}
