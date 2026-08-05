/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *    https://www.axoniq.io/pricing
 */

package io.axoniq.framework.integrationtests.multitenancy;

import io.axoniq.framework.integrationtests.testsuite.infrastructure.AxonServerTestInfrastructure;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantConnectPredicate;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.axonframework.modelling.EntityEvolver;
import org.axonframework.modelling.StateManager;
import org.axonframework.modelling.repository.Repository;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that an event-sourced entity of one tenant resumes at the correct position after its snapshot.
 *
 * @author Jan Galinski
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class MultiTenantSnapshotSourcingIT {

    private static final AxonServerTestInfrastructure INFRASTRUCTURE = AxonServerTestInfrastructure.multiTenant();
    private static final String TENANT_A = "tenant-A";
    private static final String TENANT_B = "tenant-B";
    private static final String ACCOUNT_ID = "account-1";

    private AxonServerTestInfrastructure.ContextManager contextManager;
    private AxonConfiguration application;
    private SnapshotStore snapshotStore;
    private Repository<String, Account> accounts;
    private UnitOfWorkFactory unitOfWorkFactory;
    private final AtomicInteger evolveCalls = new AtomicInteger();

    @BeforeEach
    void setUp() {
        INFRASTRUCTURE.start();
        INFRASTRUCTURE.purgeData();
        contextManager = INFRASTRUCTURE.getContextManager();
        contextManager.createContext(TENANT_A);
        contextManager.createContext(TENANT_B);

        application = EventSourcingConfigurer.create()
                                               .componentRegistry(INFRASTRUCTURE::configureInfrastructure)
                                               .componentRegistry(cr -> cr.registerComponent(
                                                       TenantResolver.class,
                                                       c -> new MetadataBasedTenantResolver()))
                                               .componentRegistry(cr -> cr.registerComponent(
                                                       TenantConnectPredicate.class,
                                                       c -> d -> !Set.of(ADMIN_CONTEXT, DEFAULT_CONTEXT)
                                                                    .contains(d.tenantId())))
                                               .registerEntity(EventSourcedEntityModule
                                                                       .declarative(String.class, Account.class)
                                                                       .messagingModel((c, model) -> model
                                                                               .entityEvolver(
                                                                                       new AccountEvolver(evolveCalls))
                                                                               .build())
                                                                       .entityFactory((id, message, context) -> {
                                                                           EventConverter converter = context.component(
                                                                                   EventConverter.class);
                                                                           AccountCreated created = message.payloadAs(
                                                                                   AccountCreated.class, converter);
                                                                           return new Account(created.id(), 0);
                                                                       })
                                                                       .criteriaResolver(c -> (id, context) ->
                                                                               EventCriteria.havingTags(
                                                                                       Tag.of("account", id)))
                                                                       // afterEvents is exclusive: after two means the
                                                                       // three events below create the snapshot.
                                                                       .snapshotPolicy(SnapshotPolicy.afterEvents(2))
                                                                       .build())
                                               .start();

        snapshotStore = application.getComponent(SnapshotStore.class);
        accounts = application.getComponent(StateManager.class).repository(Account.class, String.class);
        unitOfWorkFactory = application.getComponent(UnitOfWorkFactory.class);
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
    void sourcesEventsAppendedAfterASnapshotFromTheSameTenant() {
        publish(TENANT_A, new AccountCreated(ACCOUNT_ID));
        publish(TENANT_A, new FundsDeposited(ACCOUNT_ID, 10));
        publish(TENANT_A, new FundsDeposited(ACCOUNT_ID, 20));

        assertThat(load(TENANT_A, ACCOUNT_ID).balance()).isEqualTo(30);
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(snapshotStore.load(
                new QualifiedName(Account.class), ACCOUNT_ID, contextFor(TENANT_A)).join()).isNotNull());

        publish(TENANT_A, new FundsDeposited(ACCOUNT_ID, 30));
        publish(TENANT_A, new FundsDeposited(ACCOUNT_ID, 40));

        evolveCalls.set(0);
        assertThat(load(TENANT_A, ACCOUNT_ID).balance()).isEqualTo(100);
        assertThat(evolveCalls).hasValue(2);
    }

    private void publish(String tenantId, Object event) {
        UnitOfWork unitOfWork = unitOfWorkFactory.create();
        unitOfWork.runOnInvocation(context -> {
            context.putResource(TenantDescriptor.RESOURCE_KEY, TenantDescriptor.tenantWithId(tenantId));
            EventAppender.forContext(context).append(event);
        });
        unitOfWork.execute().orTimeout(15, TimeUnit.SECONDS).join();
    }

    private Account load(String tenantId, String accountId) {
        UnitOfWork unitOfWork = unitOfWorkFactory.create();
        return unitOfWork.executeWithResult(context -> {
            context.putResource(TenantDescriptor.RESOURCE_KEY, TenantDescriptor.tenantWithId(tenantId));
            return accounts.load(accountId, context);
        }).join().entity();
    }

    private static ProcessingContext contextFor(String tenantId) {
        return new StubProcessingContext().withResource(TenantDescriptor.RESOURCE_KEY,
                                                        TenantDescriptor.tenantWithId(tenantId));
    }

    private record AccountCreated(@EventTag(key = "account") String id) {

    }

    private record FundsDeposited(@EventTag(key = "account") String id, long amount) {

    }

    private record Account(String id, long balance) {

        private Account deposit(long amount) {
            return new Account(id, balance + amount);
        }
    }

    private static class AccountEvolver implements EntityEvolver<Account> {

        private final AtomicInteger evolveCalls;

        private AccountEvolver(AtomicInteger evolveCalls) {
            this.evolveCalls = evolveCalls;
        }

        @Override
        public Account evolve(Account account, org.axonframework.messaging.eventhandling.EventMessage event,
                              ProcessingContext context) {
            evolveCalls.incrementAndGet();
            EventConverter converter = context.component(EventConverter.class);
            if (event.type().qualifiedName().equals(new QualifiedName(AccountCreated.class))) {
                AccountCreated created = event.payloadAs(AccountCreated.class, converter);
                return new Account(created.id(), 0);
            }
            FundsDeposited deposited = event.payloadAs(FundsDeposited.class, converter);
            return account.deposit(deposited.amount());
        }
    }
}
