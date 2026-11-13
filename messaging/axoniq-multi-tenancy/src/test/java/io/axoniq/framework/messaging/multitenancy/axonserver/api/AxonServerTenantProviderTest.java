/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.axonserver.api;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.ResultStream;
import io.axoniq.axonserver.connector.admin.AdminChannel;
import io.axoniq.axonserver.grpc.admin.ContextOverview;
import io.axoniq.axonserver.grpc.admin.ContextUpdate;
import io.axoniq.axonserver.grpc.admin.ContextUpdateType;
import io.axoniq.axonserver.grpc.admin.ReplicationGroupOverview;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.MultiTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.util.FailingTenantAwareComponent;
import io.axoniq.framework.messaging.multitenancy.util.RecordingTenantAwareComponent;
import org.axonframework.common.Registration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.ADMIN_CONTEXT;
import static io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration.DEFAULT_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AxonServerTenantProviderTest {

    @Mock
    private AxonServerConnectionManager connectionManager;

    @Mock
    private AxonServerConnection connection;

    @Mock
    private AdminChannel adminChannel;

    @Mock
    private ResultStream<ContextUpdate> contextUpdates;

    @Nested
    class ContextUpdates {

        @Test
        void recordsCreatedTenantAndRemovesDeletedTenant() {
            // given
            when(connectionManager.getConnection(ADMIN_CONTEXT)).thenReturn(connection);
            when(connection.adminChannel()).thenReturn(adminChannel);
            when(adminChannel.getAllContexts()).thenReturn(CompletableFuture.completedFuture(
                    List.of(
                            contextOverview(DEFAULT_CONTEXT),
                            contextOverview(ADMIN_CONTEXT)
                    )
            ));
            when(adminChannel.subscribeToContextUpdates()).thenReturn(contextUpdates);
            when(adminChannel.getContextOverview("foo")).thenReturn(CompletableFuture.completedFuture(
                    contextOverview("foo", Map.of("region", "eu-west"))
            ));

            AxonServerTenantProvider testSubject = new AxonServerTenantProvider(
                    connectionManager,
                    tenant -> "foo".equals(tenant.tenantId())
            );
            RecordingTenantAwareComponent recorder = new RecordingTenantAwareComponent();
            testSubject.subscribe(recorder);

            // when
            testSubject.start()
                       .orTimeout(1, TimeUnit.SECONDS)
                       .join();

            // then
            assertThat(testSubject.tenants()).isEmpty();
            assertThat(recorder.tenants()).isEmpty();

            // when
            Runnable updateCallback = updateCallback();
            when(contextUpdates.nextIfAvailable()).thenReturn(contextCreated("foo"));
            updateCallback.run();

            // then
            assertThat(testSubject.tenants())
                    .extracting(TenantDescriptor::tenantId)
                    .containsExactly("foo");
            assertThat(recorder.tenants())
                    .extracting(TenantDescriptor::tenantId)
                    .containsExactly("foo");

            // when
            when(contextUpdates.nextIfAvailable()).thenReturn(contextDeleted("foo"));
            updateCallback.run();

            // then
            assertThat(testSubject.tenants()).isEmpty();
            assertThat(recorder.tenants()).isEmpty();
            verify(connectionManager).disconnect("foo");
        }

        private Runnable updateCallback() {
            ArgumentCaptor<Runnable> callback = ArgumentCaptor.forClass(Runnable.class);
            verify(contextUpdates).onAvailable(callback.capture());
            return callback.getValue();
        }
    }

    @Nested
    class Subscriptions {

        @Test
        void cancellingASubscriptionDeregistersTenantsAddedAfterSubscribing() {
            // given a component subscribed before the tenant becomes known
            AxonServerTenantProvider testSubject = new AxonServerTenantProvider(connectionManager, tenant -> true);
            RecordingTenantAwareComponent recorder = new RecordingTenantAwareComponent();
            Registration subscription = testSubject.subscribe(recorder);
            testSubject.addTenant(TenantDescriptor.tenantWithId("added-later"));
            assertThat(recorder.tenants())
                    .extracting(TenantDescriptor::tenantId)
                    .containsExactly("added-later");

            // when
            boolean cancelled = subscription.cancel();

            // then the registration made after subscribing is cancelled as well
            assertThat(cancelled).isTrue();
            assertThat(recorder.tenants()).isEmpty();
        }

        @Test
        void addingAKnownTenantAgainDoesNotRegisterItsComponentsTwice() {
            // given a subscribed component and a tenant that is already known
            AxonServerTenantProvider testSubject = new AxonServerTenantProvider(connectionManager, tenant -> true);
            RecordingTenantAwareComponent recorder = new RecordingTenantAwareComponent();
            testSubject.subscribe(recorder);
            TenantDescriptor tenant = TenantDescriptor.tenantWithId("tenant-a");
            testSubject.addTenant(tenant);

            // when the same tenant is added again
            testSubject.addTenant(tenant);

            // then the component is registered for it only once
            assertThat(recorder.tenants()).containsExactly(tenant);
        }
    }

    @Nested
    class KnownTenants {

        // Answered on every message that carries a tenant, so this provider tests its set directly rather than through
        // the list tenants() copies.
        @Test
        void onlyReportsAddedTenantsAsKnown() {
            AxonServerTenantProvider testSubject = new AxonServerTenantProvider(connectionManager, tenant -> true);
            TenantDescriptor added = TenantDescriptor.tenantWithId("tenant-a");
            testSubject.addTenant(added);

            assertThat(testSubject.isKnown(added)).isTrue();
            assertThat(testSubject.isKnown(TenantDescriptor.tenantWithId("never-added"))).isFalse();
        }

        @Test
        void stopsReportingARemovedTenantAsKnown() {
            AxonServerTenantProvider testSubject = new AxonServerTenantProvider(connectionManager, tenant -> true);
            TenantDescriptor tenant = TenantDescriptor.tenantWithId("tenant-a");
            testSubject.addTenant(tenant);

            testSubject.removeTenant(tenant);

            assertThat(testSubject.isKnown(tenant)).isFalse();
        }
    }

    @Nested
    class Shutdown {

        @Test
        void addingATenantAfterShutdownDoesNotRegisterIt() throws Exception {
            // given a subscribed component on a provider that has shut down
            AxonServerTenantProvider testSubject = new AxonServerTenantProvider(connectionManager, tenant -> true);
            RecordingTenantAwareComponent recorder = new RecordingTenantAwareComponent();
            testSubject.subscribe(recorder);
            testSubject.shutdown().get(5, TimeUnit.SECONDS);

            // when a context update still in flight adds a tenant after shutdown
            testSubject.addTenant(TenantDescriptor.tenantWithId("added-after-shutdown"));

            // then the tenant is not registered, so its instance cannot escape the shutdown cleanup
            assertThat(recorder.tenants()).isEmpty();
        }
    }

    @Nested
    class AllOrNothingRegistration {

        private static final class RegistrationError extends Error {

        }

        private static final TenantDescriptor TENANT = TenantDescriptor.tenantWithId("tenant-a");

        private final List<String> eventLog = new CopyOnWriteArrayList<>();
        private AxonServerTenantProvider testSubject;

        @BeforeEach
        void createProvider() {
            testSubject = new AxonServerTenantProvider(connectionManager, tenant -> true);
        }

        private FailingTenantAwareComponent component(String name) {
            return new FailingTenantAwareComponent(name, eventLog).observing(testSubject::isKnown,
                                                                             testSubject::tenants);
        }

        @Test
        void addingATenantThatOneComponentRejectsRollsBackEarlierRegistrationsAndWithdrawsTheTenant() {
            // given
            FailingTenantAwareComponent first = component("first");
            FailingTenantAwareComponent second = component("second").failOnRegisterNumber(1);
            FailingTenantAwareComponent third = component("third");
            testSubject.subscribe(first);
            testSubject.subscribe(second);
            testSubject.subscribe(third);

            // when / then
            assertThatThrownBy(() -> testSubject.addTenant(TENANT)).isSameAs(second.registerFailure());
            assertThat(eventLog).containsExactly("registerAndStart first tenant-a",
                                                 "registerAndStart second tenant-a",
                                                 "cancel first tenant-a");
            assertThat(third.tenants()).isEmpty();
            assertThat(testSubject.isKnown(TENANT)).isFalse();
            assertThat(testSubject.tenants()).doesNotContain(TENANT);
            assertThat(first.knownWhileRegistering()).containsExactly(true);
            assertThat(first.listedWhileRegistering()).containsExactly(true);
            assertThat(second.knownWhileRegistering()).containsExactly(true);
            assertThat(second.listedWhileRegistering()).containsExactly(true);
        }

        @Test
        void aTenantIsRoutableToComponentsWhileTheyRegisterIt() {
            // given a component that routes a message of the tenant through a router over the provider
            MetadataBasedTenantResolver resolver = new MetadataBasedTenantResolver();
            TenantRouter router = new TenantRouter(resolver, testSubject);
            List<Optional<TenantDescriptor>> routed = new ArrayList<>();
            testSubject.subscribe(new MultiTenantAwareComponent() {
                @Override
                public Registration registerTenant(TenantDescriptor tenantDescriptor) {
                    return () -> true;
                }

                @Override
                public Registration registerAndStartTenant(TenantDescriptor tenantDescriptor) {
                    Message message = resolver.attachTenant(
                            new GenericEventMessage(new MessageType("TestEvent"), "payload", Map.of()),
                            tenantDescriptor);
                    routed.add(router.resolveFromMessage(message));
                    return () -> true;
                }

                @Override
                public void describeTo(ComponentDescriptor descriptor) {
                }
            });

            // when
            testSubject.addTenant(TENANT);

            // then
            assertThat(routed).containsExactly(Optional.of(TENANT));
        }

        @Test
        void aTenantIsKnownWhileComponentsRegisterItAndWithdrawnAgainAfterARollback() {
            // given
            FailingTenantAwareComponent first = component("first");
            FailingTenantAwareComponent second = component("second");
            FailingTenantAwareComponent failing = component("failing").failOnRegisterNumber(2);
            testSubject.subscribe(first);
            testSubject.subscribe(second);

            // when
            testSubject.addTenant(TENANT);

            // then
            assertThat(first.knownWhileRegistering()).containsExactly(true);
            assertThat(first.listedWhileRegistering()).containsExactly(true);
            assertThat(second.knownWhileRegistering()).containsExactly(true);
            assertThat(second.listedWhileRegistering()).containsExactly(true);
            assertThat(testSubject.isKnown(TENANT)).isTrue();

            // when a later tenant fails
            testSubject.subscribe(failing);
            TenantDescriptor rejected = TenantDescriptor.tenantWithId("tenant-rejected");
            assertThatThrownBy(() -> testSubject.addTenant(rejected)).isSameAs(failing.registerFailure());

            // then it was known while registering, and is withdrawn again
            assertThat(failing.knownWhileRegistering()).containsExactly(true, true);
            assertThat(failing.listedWhileRegistering()).containsExactly(true, true);
            assertThat(testSubject.isKnown(rejected)).isFalse();
            assertThat(testSubject.tenants()).doesNotContain(rejected);
        }

        @Test
        void rollsBackSeveralEarlierRegistrationsInReverseOrder() {
            // given
            FailingTenantAwareComponent first = component("first");
            FailingTenantAwareComponent second = component("second");
            FailingTenantAwareComponent third = component("third").failOnRegisterNumber(1);
            testSubject.subscribe(first);
            testSubject.subscribe(second);
            testSubject.subscribe(third);

            // when / then
            assertThatThrownBy(() -> testSubject.addTenant(TENANT)).isSameAs(third.registerFailure());
            assertThat(eventLog).containsExactly("registerAndStart first tenant-a",
                                                 "registerAndStart second tenant-a",
                                                 "registerAndStart third tenant-a",
                                                 "cancel second tenant-a",
                                                 "cancel first tenant-a");
        }

        @Test
        void aRolledBackRegistrationIsNotCancelledASecondTimeOnShutdown() {
            // given
            FailingTenantAwareComponent first = component("first");
            FailingTenantAwareComponent second = component("second").failOnRegisterNumber(1);
            testSubject.subscribe(first);
            testSubject.subscribe(second);
            assertThatThrownBy(() -> testSubject.addTenant(TENANT)).isSameAs(second.registerFailure());
            eventLog.clear();

            // when
            testSubject.shutdown().orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(eventLog).isEmpty();
        }

        @Test
        void aTenantThatFailedToRegisterCanBeAddedAgainAndIsThenCancelledOncePerComponent() {
            // given
            FailingTenantAwareComponent first = component("first");
            FailingTenantAwareComponent second = component("second").failOnRegisterNumber(1);
            FailingTenantAwareComponent third = component("third");
            testSubject.subscribe(first);
            testSubject.subscribe(second);
            testSubject.subscribe(third);
            assertThatThrownBy(() -> testSubject.addTenant(TENANT)).isSameAs(second.registerFailure());
            second.stopFailing();

            // when
            testSubject.addTenant(TENANT);

            // then
            assertThat(first.tenants()).containsExactly(TENANT);
            assertThat(second.tenants()).containsExactly(TENANT);
            assertThat(third.tenants()).containsExactly(TENANT);
            assertThat(testSubject.isKnown(TENANT)).isTrue();

            // when
            eventLog.clear();
            testSubject.removeTenant(TENANT);

            // then
            assertThat(eventLog).containsExactly("cancel third tenant-a",
                                                 "cancel second tenant-a",
                                                 "cancel first tenant-a");
        }

        @Test
        void anErrorFromAComponentRollsBackEarlierRegistrationsAndIsRethrownAsIs() {
            // given
            RegistrationError error = new RegistrationError();
            FailingTenantAwareComponent first = component("first");
            FailingTenantAwareComponent second = component("second").failOnRegisterNumber(1, error);
            testSubject.subscribe(first);
            testSubject.subscribe(second);

            // when / then
            assertThatThrownBy(() -> testSubject.addTenant(TENANT)).isSameAs(error);
            assertThat(eventLog).contains("cancel first tenant-a");
            assertThat(first.tenants()).isEmpty();
            assertThat(testSubject.isKnown(TENANT)).isFalse();
        }

        @Test
        void aFailingCancelDuringRollbackDoesNotStopTheOtherCancelsAndIsSuppressed() {
            // given
            FailingTenantAwareComponent first = component("first");
            FailingTenantAwareComponent second = component("second").failOnCancelNumber(1);
            FailingTenantAwareComponent third = component("third").failOnRegisterNumber(1);
            testSubject.subscribe(first);
            testSubject.subscribe(second);
            testSubject.subscribe(third);

            // when / then
            assertThatThrownBy(() -> testSubject.addTenant(TENANT))
                    .isSameAs(third.registerFailure())
                    .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(second.cancelFailure()));
            assertThat(eventLog).containsExactly("registerAndStart first tenant-a",
                                                 "registerAndStart second tenant-a",
                                                 "registerAndStart third tenant-a",
                                                 "cancel second tenant-a",
                                                 "cancel first tenant-a");
        }

        @Test
        void aCancelFailingWithTheSameObjectAsTheRegistrationFailureDoesNotAbortTheRollback() {
            // given an earlier component whose cancel throws the very exception a later component's register throws
            IllegalStateException shared = new IllegalStateException("shared failure");
            FailingTenantAwareComponent first = component("first");
            FailingTenantAwareComponent second = component("second").failOnCancelNumber(1, shared);
            FailingTenantAwareComponent third = component("third").failOnRegisterNumber(1, shared);
            testSubject.subscribe(first);
            testSubject.subscribe(second);
            testSubject.subscribe(third);

            // when / then
            assertThatThrownBy(() -> testSubject.addTenant(TENANT)).isSameAs(shared);
            assertThat(eventLog).containsExactly("registerAndStart first tenant-a",
                                                 "registerAndStart second tenant-a",
                                                 "registerAndStart third tenant-a",
                                                 "cancel second tenant-a",
                                                 "cancel first tenant-a");
            assertThat(testSubject.isKnown(TENANT)).isFalse();
        }

        @Test
        void aTenantRemovedByAComponentWhileItRegistersIsCancelledOnceAndNotStored() {
            // given a component that removes the tenant while it is being registered
            FailingTenantAwareComponent first = component("first");
            FailingTenantAwareComponent second = component("second").onRegister(testSubject::removeTenant);
            testSubject.subscribe(first);
            testSubject.subscribe(second);

            // when
            testSubject.addTenant(TENANT);

            // then both registrations were cancelled exactly once, in reverse order, and nothing is stored
            assertThat(testSubject.isKnown(TENANT)).isFalse();
            assertThat(eventLog).containsExactly("registerAndStart first tenant-a",
                                                 "registerAndStart second tenant-a",
                                                 "cancel second tenant-a",
                                                 "cancel first tenant-a");

            // when
            eventLog.clear();
            testSubject.shutdown().orTimeout(5, TimeUnit.SECONDS).join();

            // then
            assertThat(eventLog).isEmpty();
        }

        @Test
        void aTenantRemovedByAComponentWhileItSubscribesIsCancelledAndNotStored() {
            // given two known tenants and a component that removes the first tenant it saw while registering the second
            testSubject.addTenant(TENANT);
            testSubject.addTenant(TenantDescriptor.tenantWithId("tenant-b"));
            List<TenantDescriptor> seen = new ArrayList<>();
            FailingTenantAwareComponent component = component("remover").onRegister(tenant -> {
                seen.add(tenant);
                if (seen.size() == 2) {
                    testSubject.removeTenant(seen.getFirst());
                }
            });

            // when
            testSubject.subscribe(component);

            // then the removed tenant is unknown, the component dropped it and its registration was cancelled once
            TenantDescriptor removed = seen.getFirst();
            TenantDescriptor kept = seen.getLast();
            assertThat(testSubject.isKnown(removed)).isFalse();
            assertThat(component.tenants()).containsExactly(kept);
            assertThat(eventLog).containsOnlyOnce("cancel remover " + removed.tenantId());

            // when
            eventLog.clear();
            testSubject.shutdown().orTimeout(5, TimeUnit.SECONDS).join();

            // then the removed tenant is not cancelled a second time
            assertThat(eventLog).containsExactly("cancel remover " + kept.tenantId());
        }

        @Test
        void anErrorFromACancelDuringRollbackDoesNotStopTheOtherCancelsAndIsSuppressed() {
            // given
            RegistrationError cancelError = new RegistrationError();
            FailingTenantAwareComponent first = component("first");
            FailingTenantAwareComponent second = component("second").failOnCancelNumber(1, cancelError);
            FailingTenantAwareComponent third = component("third").failOnRegisterNumber(1);
            testSubject.subscribe(first);
            testSubject.subscribe(second);
            testSubject.subscribe(third);

            // when / then
            assertThatThrownBy(() -> testSubject.addTenant(TENANT))
                    .isSameAs(third.registerFailure())
                    .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(cancelError));
            assertThat(eventLog).containsExactly("registerAndStart first tenant-a",
                                                 "registerAndStart second tenant-a",
                                                 "registerAndStart third tenant-a",
                                                 "cancel second tenant-a",
                                                 "cancel first tenant-a");
        }

        @Test
        void subscribingAComponentThatRejectsATenantCancelsItsEarlierRegistrationsAndKeepsTheProviderWorking() {
            // given
            FailingTenantAwareComponent witness = component("witness");
            testSubject.subscribe(witness);
            testSubject.addTenant(TenantDescriptor.tenantWithId("tenant-1"));
            testSubject.addTenant(TenantDescriptor.tenantWithId("tenant-2"));
            testSubject.addTenant(TenantDescriptor.tenantWithId("tenant-3"));
            FailingTenantAwareComponent component = component("component").failOnRegisterNumber(2);
            eventLog.clear();

            // when / then
            assertThatThrownBy(() -> testSubject.subscribe(component)).isSameAs(component.registerFailure());
            assertThat(eventLog).hasSize(3);
            assertThat(eventLog.get(0)).startsWith("register component ");
            assertThat(eventLog.get(2)).isEqualTo(eventLog.get(0).replace("register", "cancel"));
            assertThat(component.tenants()).isEmpty();

            // when
            eventLog.clear();
            TenantDescriptor tenant4 = TenantDescriptor.tenantWithId("tenant-4");
            testSubject.addTenant(tenant4);

            // then the failed component is not kept, the provider still serves the healthy one
            assertThat(testSubject.isKnown(tenant4)).isTrue();
            assertThat(witness.tenants()).contains(tenant4);
            assertThat(eventLog).containsExactly("registerAndStart witness tenant-4");
            assertThat(component.tenants()).isEmpty();
        }

        @Test
        void subscribeRollbackCancelsInReverseOrderAndSuppressesAFailingCancel() {
            // given
            testSubject.addTenant(TenantDescriptor.tenantWithId("tenant-1"));
            testSubject.addTenant(TenantDescriptor.tenantWithId("tenant-2"));
            testSubject.addTenant(TenantDescriptor.tenantWithId("tenant-3"));
            FailingTenantAwareComponent component = component("component").failOnRegisterNumber(3)
                                                                           .failOnCancelNumber(1);

            // when / then
            assertThatThrownBy(() -> testSubject.subscribe(component))
                    .isSameAs(component.registerFailure())
                    .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(component.cancelFailure()));
            assertThat(eventLog).hasSize(5);
            assertThat(eventLog.get(3)).isEqualTo(eventLog.get(1).replace("register", "cancel"));
            assertThat(eventLog.get(4)).isEqualTo(eventLog.get(0).replace("register", "cancel"));
        }

        @Test
        void aFailedSecondSubscriptionLeavesTheFirstSubscriptionInPlace() {
            // given
            testSubject.addTenant(TENANT);
            testSubject.addTenant(TenantDescriptor.tenantWithId("tenant-b"));
            FailingTenantAwareComponent component = component("component");
            Registration firstSubscription = testSubject.subscribe(component);
            component.failOnRegisterNumber(4);

            // when
            assertThatThrownBy(() -> testSubject.subscribe(component)).isSameAs(component.registerFailure());
            component.stopFailing();
            TenantDescriptor tenantC = TenantDescriptor.tenantWithId("tenant-c");
            testSubject.addTenant(tenantC);

            // then the first subscription still holds its tenants, and the later tenant arrives exactly once
            assertThat(component.tenants()).containsExactlyInAnyOrder(TENANT,
                                                                       TenantDescriptor.tenantWithId("tenant-b"),
                                                                       tenantC);

            // when
            firstSubscription.cancel();

            // then
            assertThat(component.tenants()).isEmpty();
        }

        @Test
        void aCreatedContextWhoseRegistrationFailsStaysUnknownAndLeavesKnownTenantsRegistered() {
            // given
            when(connectionManager.getConnection(ADMIN_CONTEXT)).thenReturn(connection);
            when(connection.adminChannel()).thenReturn(adminChannel);
            when(adminChannel.getAllContexts()).thenReturn(CompletableFuture.completedFuture(
                    List.of(contextOverview("known"))
            ));
            when(adminChannel.subscribeToContextUpdates()).thenReturn(contextUpdates);
            when(adminChannel.getContextOverview("bad")).thenReturn(CompletableFuture.completedFuture(
                    contextOverview("bad")
            ));
            FailingTenantAwareComponent first = component("first");
            FailingTenantAwareComponent second = component("second").failForTenant("bad");
            testSubject.subscribe(first);
            testSubject.subscribe(second);
            testSubject.start().orTimeout(5, TimeUnit.SECONDS).join();
            ArgumentCaptor<Runnable> callback = ArgumentCaptor.forClass(Runnable.class);
            verify(contextUpdates).onAvailable(callback.capture());
            when(contextUpdates.nextIfAvailable()).thenReturn(contextCreated("bad"));

            // when
            callback.getValue().run();

            // then
            assertThat(testSubject.tenants()).extracting(TenantDescriptor::tenantId).containsExactly("known");
            assertThat(first.tenants()).extracting(TenantDescriptor::tenantId).containsExactly("known");
            assertThat(second.tenants()).extracting(TenantDescriptor::tenantId).containsExactly("known");
        }

        @Test
        void startFailsAndLeavesNothingOfTheFailingTenantBehind() {
            // given
            when(connectionManager.getConnection(ADMIN_CONTEXT)).thenReturn(connection);
            when(connection.adminChannel()).thenReturn(adminChannel);
            when(adminChannel.getAllContexts()).thenReturn(CompletableFuture.completedFuture(
                    List.of(contextOverview("good"), contextOverview("bad"))
            ));
            FailingTenantAwareComponent first = component("first");
            FailingTenantAwareComponent second = component("second").failForTenant("bad");
            testSubject.subscribe(first);
            testSubject.subscribe(second);

            // when
            CompletableFuture<Void> started = testSubject.start();

            // then
            assertThat(started).failsWithin(5, TimeUnit.SECONDS)
                               .withThrowableOfType(ExecutionException.class)
                               .havingRootCause()
                               .isSameAs(second.registerFailure());
            assertThat(testSubject.tenants()).extracting(TenantDescriptor::tenantId).doesNotContain("bad");
            assertThat(first.tenants()).extracting(TenantDescriptor::tenantId).doesNotContain("bad");
            assertThat(second.tenants()).extracting(TenantDescriptor::tenantId).doesNotContain("bad");
        }
    }

    private static ContextOverview contextOverview(String contextName) {
        return contextOverview(contextName, Map.of());
    }

    private static ContextOverview contextOverview(String contextName,
                                                   Map<String, String> metadata) {
        return ContextOverview.newBuilder()
                              .setName(contextName)
                              .setReplicationGroup(
                                      ReplicationGroupOverview.newBuilder()
                                                              .setName("default-rg")
                                                              .build()
                              )
                              .putAllMetaData(metadata)
                              .build();
    }

    private static ContextUpdate contextCreated(String context) {
        return contextUpdate(context, ContextUpdateType.CREATED);
    }

    private static ContextUpdate contextDeleted(String context) {
        return contextUpdate(context, ContextUpdateType.DELETED);
    }

    private static ContextUpdate contextUpdate(String context,
                                               ContextUpdateType type) {
        return ContextUpdate.newBuilder()
                            .setContext(context)
                            .setType(type)
                            .build();
    }
}
