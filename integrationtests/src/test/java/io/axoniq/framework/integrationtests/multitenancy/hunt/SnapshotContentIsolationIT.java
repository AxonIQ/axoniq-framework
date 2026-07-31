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

package io.axoniq.framework.integrationtests.multitenancy.hunt;

import io.axoniq.framework.integrationtests.multitenancy.DisableMultiTenancyTestsWithoutLicense;
import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.TenantSnapshotStoreFactory;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.annotation.Snapshotting;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.modelling.annotation.InjectEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.integrationtests.multitenancy.hunt.TenantBankFixture.tenantMetadata;
import static io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.registerTenantConnectPredicate;
import static io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.registerTenantResolver;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Hunt scenario closing the snapshot residual: with a REAL snapshotting trigger active
 * ({@code @Snapshotting(afterEvents = 3)}), snapshots are actually WRITTEN and later READ, and the written content must
 * never cross tenants (claim MT-C5, "Both stay within one tenant").
 * <p>
 * ORACLE, per tenant, for the SAME entity id used in both tenants with different amounts:
 * <ol>
 *     <li>a snapshot exists in that tenant's own {@link SnapshotStore} (awaited: the write is fire-and-forget), and</li>
 *     <li>the state sourced AFTER the snapshot equals that tenant's own events only -- a snapshot written for the
 *     other tenant would surface as the other tenant's balance.</li>
 * </ol>
 * A dedicated entity is used rather than the shared bank {@code Account}, because {@code @Snapshotting} requires a
 * {@code SnapshotStore} and the single-tenant control arms run the shared entity without one.
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class SnapshotContentIsolationIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = new AxonServerTestInfrastructure();
    private static final String LEDGER_ID = "snapshotted-ledger";
    private static final Map<String, Long> OBSERVED_TOTALS = new ConcurrentHashMap<>();

    private final String runId = "hunt-" + Long.toHexString(System.nanoTime());
    private final String tenantA = runId + "-a";
    private final String tenantB = runId + "-b";

    private AxonConfiguration application;
    private AxonServerTestInfrastructure.ContextManager contextManager;

    // --- domain ---

    public record OpenLedger(String ledgerId) {

    }

    public record AddAmount(String ledgerId, long amount) {

    }

    public record ReportTotal(String ledgerId) {

    }

    public record LedgerOpened(@EventTag(key = "ledgerId") String ledgerId) {

    }

    public record AmountAdded(@EventTag(key = "ledgerId") String ledgerId, long amount) {

    }

    @EventSourcedEntity(tagKey = "ledgerId")
    @Snapshotting(afterEvents = 3)
    public static final class Ledger {

        private long total;

        @EntityCreator
        public Ledger() {
        }

        @EventSourcingHandler
        void evolve(LedgerOpened event) {
            // opening carries no amount
        }

        @EventSourcingHandler
        void evolve(AmountAdded event) {
            this.total += event.amount();
        }
    }

    /** Exposed so the single-tenant control arm can reuse the exact same domain and observation map. */
    static Map<String, Long> observedTotals() {
        return OBSERVED_TOTALS;
    }

    /** Exposed so the single-tenant control arm registers the identical handlers. */
    static org.axonframework.common.configuration.ModuleBuilder<CommandHandlingModule> ledgerCommandModule() {
        return CommandHandlingModule.named("ledger-commands")
                                    .commandHandlers()
                                    .autodetectedCommandHandlingComponent(c -> new LedgerHandlers());
    }

    static final class LedgerHandlers {

        @CommandHandler
        void handle(OpenLedger command,
                    @InjectEntity(idProperty = "ledgerId") Ledger state,
                    EventAppender appender) {
            appender.append(new LedgerOpened(command.ledgerId()));
        }

        @CommandHandler
        void handle(AddAmount command,
                    @InjectEntity(idProperty = "ledgerId") Ledger state,
                    EventAppender appender) {
            appender.append(new AmountAdded(command.ledgerId(), command.amount()));
        }

        @CommandHandler
        void handle(ReportTotal command,
                    @InjectEntity(idProperty = "ledgerId") Ledger state,
                    org.axonframework.messaging.core.unitofwork.ProcessingContext context) {
            // Keyed by tenant when multi-tenancy is active; the single-tenant control arm has no tenant on its context.
            String key = TenantDescriptor.fromContext(context)
                                         .map(TenantDescriptor::tenantId)
                                         .orElse("<single-tenant>");
            OBSERVED_TOTALS.put(key, state.total);
        }
    }

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.deleteAllCustomContexts();
        contextManager.createContext(tenantA);
        contextManager.createContext(tenantB);
        OBSERVED_TOTALS.clear();

        EventSourcingConfigurer configurer = EventSourcingConfigurer.create();
        configurer.registerEntity(EventSourcedEntityModule.autodetected(String.class, Ledger.class))
                  .registerCommandHandlingModule(ledgerCommandModule())
                  .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                  .componentRegistry(registry -> registry
                          .disableEnhancer(io.axoniq.license.entitlement.EntitlementConfigurationEnhancer.class)
                          .disableEnhancer(io.axoniq.license.entitlement.source.axonserver.AxonServerLicenseSourceConfigurationEnhancer.class))
                  // Multi-tenancy supplies the per-tenant SnapshotStore itself; registering one here is rejected
                  // on purpose by AxonServerMultiTenancyConfigurationDefaults#rejectForeignSnapshotStore.
                  .componentRegistry(MultiTenancyEnabled::enableMultiTenancyEnhancer)
                  .componentRegistry(registerTenantResolver(new MetadataBasedTenantResolver()))
                  .componentRegistry(registerTenantConnectPredicate(d -> d.tenantId().startsWith(runId)));
        application = configurer.start();
    }

    @AfterEach
    void tearDown() {
        if (application != null) {
            application.shutdown();
        }
        INFRASTRUCTURE.purgeData();
        contextManager.deleteAllCustomContexts();
        INFRASTRUCTURE.stop();
    }

    @Test
    void snapshotsAreWrittenPerTenantAndTheirContentNeverCrossesTenants() {
        CommandGateway gateway = application.getComponent(CommandGateway.class);

        // given the same ledger id in both tenants, past the 3-event snapshot trigger, with distinct amounts
        send(gateway, new OpenLedger(LEDGER_ID), tenantA);
        send(gateway, new OpenLedger(LEDGER_ID), tenantB);
        for (int i = 0; i < 5; i++) {
            send(gateway, new AddAmount(LEDGER_ID, 1L), tenantA);
            send(gateway, new AddAmount(LEDGER_ID, 1_000L), tenantB);
        }

        // then each tenant's own snapshot store holds a snapshot for the entity (the write is fire-and-forget)
        TenantSnapshotStoreFactory factory = application.getComponent(TenantSnapshotStoreFactory.class);
        SnapshotStore storeA = factory.storeFor(TenantDescriptor.tenantWithId(tenantA));
        SnapshotStore storeB = factory.storeFor(TenantDescriptor.tenantWithId(tenantB));
        QualifiedName entityName = new QualifiedName(Ledger.class);

        await().atMost(Duration.ofSeconds(20))
               .untilAsserted(() -> assertThat(loadSnapshot(storeA, entityName))
                       .as("tenant A must have a snapshot of its own ledger after passing the trigger")
                       .isNotNull());
        await().atMost(Duration.ofSeconds(20))
               .untilAsserted(() -> assertThat(loadSnapshot(storeB, entityName))
                       .as("tenant B must have a snapshot of its own ledger after passing the trigger")
                       .isNotNull());

        // and the state sourced from that snapshot plus later events is each tenant's own, never the other's
        send(gateway, new ReportTotal(LEDGER_ID), tenantA);
        send(gateway, new ReportTotal(LEDGER_ID), tenantB);
        assertThat(OBSERVED_TOTALS.get(tenantA))
                .as("tenant A sourced through its snapshot must see only its 5 x 1")
                .isEqualTo(5L);
        assertThat(OBSERVED_TOTALS.get(tenantB))
                .as("tenant B sourced through its snapshot must see only its 5 x 1000")
                .isEqualTo(5_000L);
    }

    private static Snapshot loadSnapshot(SnapshotStore store, QualifiedName entityName) {
        return store.load(entityName, LEDGER_ID, null)
                    .orTimeout(10, TimeUnit.SECONDS)
                    .join();
    }

    private static void send(CommandGateway gateway, Object command, String tenant) {
        gateway.send(command, tenantMetadata(tenant), null)
               .getResultMessage()
               .orTimeout(30, TimeUnit.SECONDS)
               .join();
    }
}
