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
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandDispatcher;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.QueryBus;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.axonframework.messaging.queryhandling.QueryUpdateEmitter;
import org.axonframework.messaging.queryhandling.annotation.QueryHandler;
import org.axonframework.messaging.queryhandling.configuration.QueryHandlingModule;
import org.axonframework.modelling.annotation.InjectEntity;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;

/**
 * Production-shaped multi-tenant banking application for the multi-tenancy hunt: an event-sourced {@code Account}
 * entity per tenant on a real multi-context Axon Server, commands through the distributed command bus, a tenant-scoped
 * read store maintained from the command side (this branch has no multi-tenant streaming read side), and query /
 * subscription-query support.
 * <p>
 * The domain mirrors the university multi-tenancy example's structure (autodetected event-sourced entity, command
 * handling module, tenant-scoped components), reduced to a conservation-law-friendly bank account.
 */
public final class TenantBankFixture {

    public static final String TENANT_A = "hunt-tenant-a";
    public static final String TENANT_B = "hunt-tenant-b";

    /** Set by the {@link SlowQuery} handler on entry, so a test can force a disconnect while a query is in flight. */
    public static final java.util.concurrent.atomic.AtomicBoolean slowQueryEntered =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    /** Subscription streams opened by {@link StartWatch} handlers, keyed by account id, for tests to consume. */
    public static final Map<String, MessageStream<QueryResponseMessage>> contextRoutedSubscriptions =
            new ConcurrentHashMap<>();

    private TenantBankFixture() {
    }

    // --- messages ---

    public record OpenAccount(String accountId) {

    }

    public record DepositMoney(String accountId, long amount) {

    }

    /** Reads the sourced state of the account and records the observed balance in the tenant's store. */
    public record RecordBalance(String accountId) {

    }

    /** Dispatches a follow-up {@link DepositMoney} from inside the handler, without naming its tenant. */
    public record ChainDeposit(String accountId, long amount) {

    }

    /** Emits a subscription-query update for the account; optionally fails the unit of work afterwards. */
    public record EmitBalanceUpdate(String accountId, long value, boolean failAfterEmit) {

    }

    public record AccountOpened(@EventTag(key = "accountId") String accountId) {

    }

    public record MoneyDeposited(@EventTag(key = "accountId") String accountId, long amount) {

    }

    public record BalanceQuery(String accountId) {

    }

    /** Subscription-query target whose handler needs no tenant, so the initial result works on any routing path. */
    public record WatchAccount(String accountId) {

    }

    /** Query whose handler signals entry and then sleeps, so a disconnect can be forced while it is in flight. */
    public record SlowQuery(long sleepMs) {

    }

    /** Opens a subscription query for {@link WatchAccount} from INSIDE the handler, tenant taken from the context. */
    public record StartWatch(String accountId) {

    }

    /** Deposit whose handler signals entry through the tenant store and then sleeps before appending. */
    public record SlowDeposit(String accountId, long amount, long sleepMs) {

    }

    // --- tenant-scoped read store, maintained from the command side ---

    public static final class BalanceStore {

        private final Map<String, Long> observedBalances = new ConcurrentHashMap<>();
        private final Map<String, String> notes = new ConcurrentHashMap<>();

        public void recordBalance(String accountId, long balance) {
            observedBalances.put(accountId, balance);
        }

        public Long observedBalance(String accountId) {
            return observedBalances.get(accountId);
        }

        public void note(String key, String value) {
            notes.put(key, value);
        }

        public String note(String key) {
            return notes.get(key);
        }
    }

    // --- entity ---

    @EventSourcedEntity(tagKey = "accountId")
    public static final class Account {

        private boolean open;
        private long balance;

        @EntityCreator
        Account() {
        }

        @EventSourcingHandler
        void evolve(AccountOpened event) {
            this.open = true;
        }

        @EventSourcingHandler
        void evolve(MoneyDeposited event) {
            this.balance += event.amount();
        }

        boolean isOpen() {
            return open;
        }
    }

    // --- command handlers ---

    static final class BankCommandHandlers {

        @CommandHandler
        void handle(OpenAccount command,
                    @Nullable @InjectEntity(idProperty = "accountId") Account state,
                    EventAppender appender) {
            if (state == null || !state.open) {
                appender.append(new AccountOpened(command.accountId()));
            }
        }

        @CommandHandler
        void handle(DepositMoney command,
                    @InjectEntity(idProperty = "accountId") Account state,
                    EventAppender appender,
                    @TenantScoped BalanceStore store,
                    ProcessingContext context) {
            if (!state.open) {
                throw new IllegalStateException("Account [" + command.accountId() + "] is not open in tenant ["
                                                        + TenantDescriptor.fromContext(context)
                                                                          .map(TenantDescriptor::tenantId)
                                                                          .orElse("<none>") + "]");
            }
            appender.append(new MoneyDeposited(command.accountId(), command.amount()));
            store.recordBalance(command.accountId(), state.balance + command.amount());
        }

        @CommandHandler
        void handle(RecordBalance command,
                    @InjectEntity(idProperty = "accountId") Account state,
                    @TenantScoped BalanceStore store) {
            store.recordBalance(command.accountId(), state.balance);
            store.note("open-" + command.accountId(), Boolean.toString(state.open));
        }

        @CommandHandler
        void handle(ChainDeposit command,
                    CommandDispatcher dispatcher,
                    @TenantScoped BalanceStore store) {
            try {
                dispatcher.send(new DepositMoney(command.accountId(), command.amount()))
                          .getResultMessage()
                          .orTimeout(10, TimeUnit.SECONDS)
                          .join();
                store.note("chain-" + command.accountId(), "ok");
            } catch (Exception e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                store.note("chain-" + command.accountId(), "failed: " + cause.getClass().getSimpleName()
                        + ": " + cause.getMessage());
            }
        }

        @CommandHandler
        void handle(StartWatch command, ProcessingContext context) {
            // A subscription query dispatched while handling another message: the tenant travels in the context, the
            // query message itself names no tenant, mirroring the follow-up-command convenience the connectors offer.
            MessageStream<QueryResponseMessage> stream = context.component(QueryBus.class)
                    .subscriptionQuery(new GenericQueryMessage(new MessageType(WatchAccount.class),
                                                               new WatchAccount(command.accountId())),
                                       context,
                                       50);
            contextRoutedSubscriptions.put(command.accountId(), stream);
        }

        @CommandHandler
        void handle(SlowDeposit command,
                    @InjectEntity(idProperty = "accountId") Account state,
                    EventAppender appender,
                    @TenantScoped BalanceStore store) throws InterruptedException {
            store.note("slow-" + command.accountId(), "started");
            Thread.sleep(command.sleepMs());
            appender.append(new MoneyDeposited(command.accountId(), command.amount()));
        }

        @CommandHandler
        void handle(EmitBalanceUpdate command, ProcessingContext context) {
            QueryUpdateEmitter.forContext(context)
                              .emit(WatchAccount.class,
                                    query -> query.accountId().equals(command.accountId()),
                                    command.value());
            if (command.failAfterEmit()) {
                throw new IllegalStateException("Deliberate failure after emitting update for ["
                                                        + command.accountId() + "]");
            }
        }
    }

    // --- query handler ---

    static final class BankQueryHandlers {

        @QueryHandler
        public Long handle(BalanceQuery query, @TenantScoped BalanceStore store) {
            Long observed = store.observedBalance(query.accountId());
            return observed == null ? -1L : observed;
        }

        /** Tenant-free handler, so the initial result of a {@link WatchAccount} subscription works on any routing path. */
        @QueryHandler
        public Long handle(WatchAccount query) {
            return 0L;
        }

        @QueryHandler
        public Long handle(SlowQuery query) throws InterruptedException {
            slowQueryEntered.set(true);
            Thread.sleep(query.sleepMs());
            return query.sleepMs();
        }
    }

    // --- application assembly ---

    /**
     * Starts the bank application against the given infrastructure. Per-tenant {@link BalanceStore} instances are
     * exposed through {@code storesOut}, keyed by tenant id, so tests can observe each tenant's store directly.
     */
    public static AxonConfiguration startApp(AxonServerTestInfrastructure infrastructure,
                                             Map<String, BalanceStore> storesOut,
                                             Consumer<ComponentRegistry> extraRegistry) {
        return startApp(infrastructure, storesOut, "hunt-", extraRegistry);
    }

    /**
     * Starts the bank application accepting only tenants whose id starts with {@code tenantPrefix}. Tests use a
     * per-test prefix so an application instance that outlives its test (e.g. after a timed-out shutdown) never
     * joins a later test's contexts through the dynamic context-update subscription.
     */
    public static AxonConfiguration startApp(AxonServerTestInfrastructure infrastructure,
                                             Map<String, BalanceStore> storesOut,
                                             String tenantPrefix,
                                             Consumer<ComponentRegistry> extraRegistry) {
        return startApp(infrastructure, storesOut, tenantPrefix, true, extraRegistry);
    }

    /**
     * Starts the bank application, optionally WITHOUT the emitting command handlers. Pinning the
     * {@link EmitBalanceUpdate} handler to a single instance is what makes a genuine cross-node emission test
     * possible: Axon Server routes the command to the only instance that subscribed it.
     */
    public static AxonConfiguration startApp(AxonServerTestInfrastructure infrastructure,
                                             Map<String, BalanceStore> storesOut,
                                             String tenantPrefix,
                                             boolean withCommandHandlers,
                                             Consumer<ComponentRegistry> extraRegistry) {
        EventSourcingConfigurer configurer = EventSourcingConfigurer.create();
        configurer.registerEntity(EventSourcedEntityModule.autodetected(String.class, Account.class));
        if (withCommandHandlers) {
            configurer.registerCommandHandlingModule(
                    CommandHandlingModule.named("bank-commands")
                                         .commandHandlers()
                                         .autodetectedCommandHandlingComponent(c -> new BankCommandHandlers()));
        }
        configurer.registerQueryHandlingModule(
                          QueryHandlingModule.named("bank-queries")
                                             .queryHandlers()
                                             .autodetectedQueryHandlingComponent(c -> new BankQueryHandlers())
                                             .build())
                  .componentRegistry(infrastructure::configureInfrastructure)
                  // The local Axon Server test license is an RSA-signed AxonServer license; entitlement-manager
                  // 1.1.0 expects an Ed25519 signature, exits the VM on validation failure when it pulls that
                  // license from the server, and its watchdog kills the JVM after the no-license grace period.
                  // Client-side entitlement enforcement is switched off for the hunt; the server keeps its license.
                  .componentRegistry(registry -> registry
                          .disableEnhancer(io.axoniq.license.entitlement.EntitlementConfigurationEnhancer.class)
                          .disableEnhancer(io.axoniq.license.entitlement.source.axonserver.AxonServerLicenseSourceConfigurationEnhancer.class))
                  .componentRegistry(registry -> registry.registerComponent(
                          TenantConnectPredicate.class,
                          config -> descriptor -> !Set.of(ADMIN_CONTEXT, DEFAULT_CONTEXT)
                                                      .contains(descriptor.tenantId())
                                                  && descriptor.tenantId().startsWith(tenantPrefix)))
                  .componentRegistry(registry -> registry.registerComponent(
                          TenantComponentProvider.class,
                          config -> TenantComponentProvider.withFactory(
                                  BalanceStore.class,
                                  tenant -> storesOut.computeIfAbsent(tenant.tenantId(), id -> new BalanceStore()))))
                  .componentRegistry(extraRegistry);
        return configurer.start();
    }

    public static Metadata tenantMetadata(String tenantId) {
        return Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, tenantId);
    }
}
