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

package io.axoniq.framework.springcloud.command;

import io.axoniq.framework.springcloud.discovery.RecordingCapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.routing.Member;
import io.axoniq.framework.springcloud.routing.MemberCapabilities;
import io.axoniq.framework.springcloud.shared.SpringCloudAxoniqAddon;
import io.axoniq.framework.springcloud.shared.SpringCloudMemberRegistry;
import io.axoniq.framework.springcloud.util.RecordingCommandHandler;
import io.axoniq.framework.springcloud.util.RecordingDiscoveryClient;
import io.axoniq.framework.springcloud.util.RecordingEntitlementManager;
import io.axoniq.framework.springcloud.util.TestServiceInstance;
import io.axoniq.license.entitlement.EntitlementMessageType;
import org.axonframework.messaging.commandhandling.CommandDispatchException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the routing and subscription behaviour of {@link SpringCloudCommandBusConnector}.
 *
 * @author Allard Buijze
 */
class SpringCloudCommandBusConnectorTest {

    private static final MessageType CREATE_COURSE_TYPE = new MessageType("university.CreateCourse", "1.0.0");
    private static final QualifiedName CREATE_COURSE = CREATE_COURSE_TYPE.qualifiedName();
    private static final QualifiedName RENAME_COURSE = new QualifiedName("university.RenameCourse");
    private static final MessageType RESULT_TYPE = new MessageType("university.CourseId", "1.0.0");
    private static final byte[] PAYLOAD = "{\"name\":\"Axon 5\"}".getBytes(StandardCharsets.UTF_8);
    private static final int LOAD_FACTOR = 100;

    private TestServiceInstance localInstance;
    private TestServiceInstance remoteInstance;
    private RecordingDiscoveryClient discoveryClient;
    private RecordingCapabilityDiscoveryMode discoveryMode;
    private SpringCloudMemberRegistry registry;
    private RecordingCommandHandler handler;
    private RecordingRemoteCommandDispatcher dispatcher;
    private RecordingEntitlementManager entitlementManager;
    private SpringCloudCommandBusConnector testSubject;

    @BeforeEach
    void setUp() {
        localInstance = TestServiceInstance.instance("university", "node-a", 8080);
        remoteInstance = TestServiceInstance.instance("university", "node-b", 8080);
        discoveryClient = new RecordingDiscoveryClient().register("university", localInstance);
        discoveryMode = new RecordingCapabilityDiscoveryMode().answeringAsLocal(localInstance);
        registry = new SpringCloudMemberRegistry(discoveryClient, discoveryMode);
        handler = new RecordingCommandHandler();
        dispatcher = new RecordingRemoteCommandDispatcher();
        entitlementManager = new RecordingEntitlementManager();
        testSubject = new SpringCloudCommandBusConnector(registry,
                                                         new IncomingCommandInvoker(() -> "node-a", null),
                                                         dispatcher,
                                                         null,
                                                         entitlementManager);
        testSubject.onIncomingCommand(handler);
        testSubject.start();
    }

    private static CommandMessage command(MessageType type, String routingKey) {
        return new GenericCommandMessage(
                new GenericMessage("command-1", type, PAYLOAD, Map.of()), routingKey, null
        );
    }

    /**
     * Puts the remote instance on the ring as a member handling the given {@code commands}, as a discovery round
     * would.
     */
    private Member discoverRemoteMemberHandling(QualifiedName... commands) {
        discoveryClient.register("university", remoteInstance);
        discoveryMode.answering(remoteInstance, new MemberCapabilities(LOAD_FACTOR, Set.of(commands), Set.of()));
        registry.updateMemberships();
        return registry.ring().members().stream()
                       .filter(member -> !member.local())
                       .findFirst()
                       .orElseThrow();
    }

    @Nested
    class RoutingToThisMember {

        @Test
        void handsTheCommandToTheRegisteredHandler() {
            // given
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();

            // when
            testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join();

            // then — a command routed here goes through the registered handler, which owns the priority executor
            // and the local segment; the local CommandBus is never called directly
            assertThat(handler.handled()).hasSize(1);
            assertThat(dispatcher.dispatches()).isEmpty();
        }

        @Test
        void completesWithTheHandlerResult() {
            // given
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();
            handler.answeringWith(new GenericCommandResultMessage(RESULT_TYPE, PAYLOAD));

            // when
            CommandResultMessage result = testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join();

            // then
            assertThat(result).isNotNull();
            assertThat(result.type()).isEqualTo(RESULT_TYPE);
        }

        @Test
        void completesWithoutAResultWhenTheHandlerReturnedNone() {
            // given
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();

            // when
            CompletableFuture<CommandResultMessage> result =
                    testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null);

            // then
            assertThat(result).isCompleted();
            assertThat(result.join()).isNull();
        }

        @Test
        void completesExceptionallyWhenTheHandlerFails() {
            // given
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();
            handler.failingWith(new IllegalStateException("course is full"));

            // when / then
            assertThatThrownBy(() -> testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseInstanceOf(IllegalStateException.class)
                    .hasRootCauseMessage("course is full");
        }

        @Test
        void routesByRoutingKeyRatherThanByCommand() {
            // given
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();

            // when
            testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join();
            testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-2"), null).join();

            // then — both land here because this is the only member; what matters is that the key is what is routed on
            assertThat(handler.handled()).hasSize(2);
            assertThat(handler.handled()).extracting(message -> message.routingKey().orElseThrow())
                                         .containsExactly("course-1", "course-2");
        }

        @Test
        void routesACommandWithoutARoutingKey() {
            // given
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();
            CommandMessage unrouted = new GenericCommandMessage(
                    new GenericMessage("command-1", CREATE_COURSE_TYPE, PAYLOAD, Map.of())
            );

            // when
            testSubject.dispatch(unrouted, null).join();

            // then — falling back to the identifier lands it on an arbitrary but valid member, rather than failing
            assertThat(handler.handled()).hasSize(1);
        }
    }

    @Nested
    class RoutingToAnotherMember {

        @Test
        void sendsTheCommandToTheMemberThatHandlesIt() {
            // given — only the remote member subscribed to this command
            Member remote = discoverRemoteMemberHandling(CREATE_COURSE);

            // when
            testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join();

            // then
            assertThat(dispatcher.members()).containsExactly(remote);
            assertThat(handler.handled()).isEmpty();
        }

        @Test
        void completesWithTheResultTheOtherMemberReturned() {
            // given
            discoverRemoteMemberHandling(CREATE_COURSE);
            dispatcher.answeringWith(new GenericCommandResultMessage(RESULT_TYPE, PAYLOAD));

            // when
            CommandResultMessage result = testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join();

            // then
            assertThat(result).isNotNull();
            assertThat(result.type()).isEqualTo(RESULT_TYPE);
        }

        @Test
        void takesAnUnreachableMemberOutOfTheRing() {
            // given
            Member remote = discoverRemoteMemberHandling(CREATE_COURSE);
            dispatcher.failingWith(new MemberUnreachableException("connection refused"));

            // when
            assertThatThrownBy(() -> testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join())
                    .isInstanceOf(CompletionException.class);

            // then
            assertThat(registry.ring().members()).doesNotContain(remote);
        }

        @Test
        void leavesAMemberThatReportedADispatchFailureOnTheRing() {
            // given — the member was reached and answered promptly, saying it could not read the command. That
            // arrives as a plain CommandDispatchException, which must not be mistaken for a member it could not
            // reach: evicting it would hand the same unreadable command to the next member, and so on until the
            // ring is empty.
            Member remote = discoverRemoteMemberHandling(CREATE_COURSE);
            dispatcher.failingWith(new CommandDispatchException(
                    "Could not read incoming command of type [university.CreateCourse]."
            ));

            // when
            assertThatThrownBy(() -> testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join())
                    .isInstanceOf(CompletionException.class);

            // then
            assertThat(registry.ring().members()).contains(remote);
        }

        @Test
        void leavesAMemberWhoseHandlerFailedOnTheRing() {
            // given
            Member remote = discoverRemoteMemberHandling(CREATE_COURSE);
            dispatcher.failingWith(new IllegalStateException("course is full"));

            // when
            assertThatThrownBy(() -> testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join())
                    .isInstanceOf(CompletionException.class);

            // then — a failing handler says nothing about availability; removing the member would move a failing
            // command onto every other member in turn
            assertThat(registry.ring().members()).contains(remote);
        }
    }

    @Nested
    class WithNoMemberHandlingTheCommand {

        @Test
        void failsWithNoHandlerForCommand() {
            // given — nothing subscribed to this command anywhere
            testSubject.subscribe(RENAME_COURSE, LOAD_FACTOR).join();

            // when / then
            assertThatThrownBy(() -> testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join())
                    .isInstanceOf(CompletionException.class)
                    .hasRootCauseInstanceOf(NoHandlerForCommandException.class);
        }

        @Test
        void doesNotClaimAnythingAgainstTheEntitlement() {
            // when
            assertThatThrownBy(() -> testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join())
                    .isInstanceOf(CompletionException.class);

            // then — a command that was never routed anywhere was never distributed either
            assertThat(entitlementManager.claims()).isEmpty();
        }
    }

    @Nested
    class Subscribing {

        @Test
        void completesWithoutWaitingForOtherMembersToNotice() {
            // when
            CompletableFuture<Void> subscribed = testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR);

            // then — DistributedCommandBus joins this on the subscribing thread, so it must not wait on discovery
            assertThat(subscribed).isCompleted();
        }

        @Test
        void publishesTheSubscribedCommandAsACapability() {
            // when
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();

            // then
            assertThat(discoveryMode.localCapabilities().commands()).containsExactly(CREATE_COURSE);
            assertThat(discoveryMode.localCapabilities().loadFactor()).isEqualTo(LOAD_FACTOR);
        }

        @Test
        void publishesEverySubscribedCommand() {
            // when
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();
            testSubject.subscribe(RENAME_COURSE, LOAD_FACTOR).join();

            // then
            assertThat(discoveryMode.localCapabilities().commands())
                    .containsExactlyInAnyOrder(CREATE_COURSE, RENAME_COURSE);
        }

        @Test
        void adoptsTheLoadFactorOfAResubscription() {
            // given
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();

            // when
            testSubject.subscribe(CREATE_COURSE, 250).join();

            // then
            assertThat(discoveryMode.localCapabilities().loadFactor()).isEqualTo(250);
            assertThat(discoveryMode.localCapabilities().commands()).containsExactly(CREATE_COURSE);
        }

        @Test
        void rejectsANegativeLoadFactor() {
            // when — reported through the returned future, not thrown, so a caller composing on it sees the failure
            CompletableFuture<Void> subscribed = testSubject.subscribe(CREATE_COURSE, -1);

            // then
            assertThat(subscribed).isCompletedExceptionally();
            assertThatThrownBy(subscribed::join)
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot be negative");
            // and the rejected subscription was not recorded
            assertThat(discoveryMode.localCapabilities().commands()).doesNotContain(CREATE_COURSE);
        }

        @Test
        void rejectsANullCommandName() {
            // when / then
            assertThatThrownBy(() -> testSubject.subscribe(null, LOAD_FACTOR))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    class Unsubscribing {

        @Test
        void stopsPublishingTheCommandAsACapability() {
            // given
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();
            testSubject.subscribe(RENAME_COURSE, LOAD_FACTOR).join();

            // when
            boolean unsubscribed = testSubject.unsubscribe(CREATE_COURSE);

            // then
            assertThat(unsubscribed).isTrue();
            assertThat(discoveryMode.localCapabilities().commands()).containsExactly(RENAME_COURSE);
        }

        @Test
        void makesTheCommandUnroutable() {
            // given
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();

            // when
            testSubject.unsubscribe(CREATE_COURSE);

            // then
            assertThatThrownBy(() -> testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join())
                    .hasRootCauseInstanceOf(NoHandlerForCommandException.class);
        }

        @Test
        void reportsThatNothingWasSubscribed() {
            // when / then
            assertThat(testSubject.unsubscribe(CREATE_COURSE)).isFalse();
        }

        @Test
        void undoesAnyNumberOfSubscriptions() {
            // given
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();

            // when
            boolean unsubscribed = testSubject.unsubscribe(CREATE_COURSE);

            // then — the contract says one unsubscribe undoes any number of subscriptions
            assertThat(unsubscribed).isTrue();
            assertThat(discoveryMode.localCapabilities().commands()).isEmpty();
        }
    }

    @Nested
    class Entitlement {

        @Test
        void claimsOneCommandPerDispatch() {
            // given
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();

            // when
            testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join();
            testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-2"), null).join();

            // then
            assertThat(entitlementManager.claims())
                    .allSatisfy(claim -> {
                        assertThat(claim.addonIdentifier()).isEqualTo(SpringCloudAxoniqAddon.IDENTIFIER);
                        assertThat(claim.messageType()).isEqualTo(EntitlementMessageType.COMMAND);
                        assertThat(claim.count()).isEqualTo(1);
                    })
                    .hasSize(2);
        }

        @Test
        void claimsOnceForACommandRoutedToAnotherMember() {
            // given
            discoverRemoteMemberHandling(CREATE_COURSE);

            // when
            testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null).join();

            // then — the dispatching member claims; the receiving member must not claim the same command again
            assertThat(entitlementManager.claims()).hasSize(1);
        }
    }

    @Nested
    class ShuttingDown {

        @Test
        void tellsOtherMembersItHandlesNothing() {
            // given
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();

            // when
            testSubject.disconnect().join();

            // then — members still running route around this one on their next discovery round
            assertThat(discoveryMode.localCapabilities().commands()).isEmpty();
        }

        @Test
        void refusesNewCommandsOnceDispatchingHasShutDown() {
            // given
            testSubject.subscribe(CREATE_COURSE, LOAD_FACTOR).join();
            testSubject.shutdownDispatching().join();

            // when / then
            assertThatThrownBy(() -> testSubject.dispatch(command(CREATE_COURSE_TYPE, "course-1"), null))
                    .hasMessageContaining("shutting down");
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsNullCollaborators() {
            // given
            IncomingCommandInvoker invoker = new IncomingCommandInvoker(() -> "node-a", null);

            // when / then
            assertThatThrownBy(() -> new SpringCloudCommandBusConnector(
                    null, invoker, dispatcher, null, entitlementManager
            )).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> new SpringCloudCommandBusConnector(
                    registry, null, dispatcher, null, entitlementManager
            )).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> new SpringCloudCommandBusConnector(
                    registry, invoker, null, null, entitlementManager
            )).isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsANullCommandOrHandler() {
            // when / then
            assertThatThrownBy(() -> testSubject.dispatch(null, null)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> testSubject.onIncomingCommand(null)).isInstanceOf(NullPointerException.class);
        }
    }
}
