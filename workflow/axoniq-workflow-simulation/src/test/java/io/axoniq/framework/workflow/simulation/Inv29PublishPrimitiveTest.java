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
package io.axoniq.framework.workflow.simulation;

import io.axoniq.framework.workflow.simulation.scenarios.PublishChainScenario;
import io.axoniq.framework.workflow.simulation.scenarios.PublishCrashScenario;
import io.axoniq.framework.workflow.simulation.scenarios.PublishFanOutWakeScenario;
import io.axoniq.framework.workflow.simulation.scenarios.PublishToEventHandlerScenario;
import io.axoniq.framework.workflow.simulation.scenarios.PublishWaitOrderScenario;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishReplyEvent;
import io.axoniq.framework.workflow.simulation.workflow.SimulationEvents.PublishRequestEvent;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * INV-29 ({@code NoForeignStepRecorded}) and INV-30 ({@code PublisherObservesOwnPublish}) for the publish primitive
 * (ADR-019), driven through the deterministic publish scenarios. The fuzz tiers drive the same chain under the fault
 * set; these pins name each vector: the chain itself, wait-before and wait-after ordering, the two crash windows, and a
 * plain Axon event handler as consumer.
 *
 * @author Stefan Dragisic
 */
class Inv29PublishPrimitiveTest {

    private static final Logger logger = LoggerFactory.getLogger(Inv29PublishPrimitiveTest.class);

    @Nested
    class ChainOfPublishedEvents {

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void onePublishedEventIsOneRecordThatStartsTwoWorkflowsAndWakesThePreRegisteredWait() {
            var outcome = PublishChainScenario.run(7L, "c1");
            logger.info("Publish chain: {}", outcome);
            assertThat(outcome.requestRecords()).as("the requester's publish is exactly one durable record").isEqualTo(1);
            assertThat(outcome.replyRecords()).as("the responder's publish is exactly one durable record").isEqualTo(1);
            assertThat(outcome.requestRecordType())
                    .as("the record is the business event itself, under its own type")
                    .isEqualTo(new QualifiedName(PublishRequestEvent.class).toString());
            assertThat(outcome.replyRecordType()).isEqualTo(new QualifiedName(PublishReplyEvent.class).toString());
            assertThat(outcome.responderStarts()).as("one published event starts the responder once").isEqualTo(1);
            assertThat(outcome.observerStarts()).as("the same published event starts the observer once").isEqualTo(1);
            assertThat(outcome.requesterHoldsPublish()).as("the publisher's own state holds the publish step").isTrue();
            assertThat(outcome.requesterWaitCompleted())
                    .as("the requester's wait, registered before the publish, was woken by the published reply")
                    .isTrue();
            assertThat(outcome.effectsOnce()).as("every counting effect of the chain ran exactly once").isTrue();
        }

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void noInstanceRegistersAnotherPublishersStepAndNothingChangesAcrossCrashAndReplay() {
            var outcome = PublishChainScenario.run(11L, "c2");
            assertThat(outcome.responderHoldsRequest())
                    .as("the responder, started by the published request, must not register the requester's step")
                    .isFalse();
            assertThat(outcome.observerHoldsRequest())
                    .as("the observer, started by the published request, must not register the requester's step")
                    .isFalse();
            assertThat(outcome.requesterHoldsReply())
                    .as("the requester, woken by the published reply, must not register the responder's step")
                    .isFalse();
            assertThat(outcome.stableAfterCrash())
                    .as("a crash after the chain is durable re-sources the same records, starts and step lists")
                    .isTrue();
        }
    }

    @Nested
    class WaitOrdering {

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void aWaitRegisteredBeforeThePublishIsWokenExactlyOnce() {
            var outcome = PublishWaitOrderScenario.waiterRegisteredBeforePublish(3L, "w1");
            logger.info("Wait before publish: {}", outcome);
            assertThat(outcome.waitCompletions()).as("the published event completes the parked wait once").isEqualTo(1);
            assertThat(outcome.afterRequestRuns()).as("the waiter continued past its wait once").isEqualTo(1);
            assertThat(outcome.waiterTerminal()).isTrue();
            assertThat(outcome.waiterHoldsForeign()).as("the woken waiter holds none of the publisher's steps").isFalse();
        }

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void aWaitRegisteredAfterThePublishedEventPassedIsNotWoken_documentedSemantics() {
            var outcome = PublishWaitOrderScenario.waiterRegisteredAfterPublish(5L, "w2");
            logger.info("Wait after publish: {}", outcome);
            assertThat(outcome.publishedBeforeWait()).as("or this probe proves nothing").isTrue();
            assertThat(outcome.waiterParked()).isTrue();
            assertThat(outcome.wokenWithinWindow())
                    .as("wait conditions are evaluated at live delivery only: an event that went by before the wait "
                                + "was registered does not wake it (engine semantics for any event, published or not)")
                    .isFalse();
        }
    }

    @Nested
    class RunningToRunning {

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void oneEventPublishedByARunningWorkflowWakesThreeRunningWaitersOnceEachAndNotTheFourth() {
            var outcome = PublishFanOutWakeScenario.run(31L, "f1");
            logger.info("Publish fan-out wake: {}", outcome);
            assertThat(outcome.waitCompletionsPerWaiter())
                    .as("each parked waiter's wait completes exactly once on the one published event")
                    .containsExactly(1, 1, 1);
            assertThat(outcome.afterRequestRunsPerWaiter())
                    .as("each woken waiter continues past its wait exactly once")
                    .containsExactly(1, 1, 1);
            assertThat(outcome.allWaitersTerminal()).isTrue();
            assertThat(outcome.controlWaiterWoken())
                    .as("a waiter on another order id is not woken by this published event (correlation holds)")
                    .isFalse();
            assertThat(outcome.anyWaiterHoldsForeign())
                    .as("no woken waiter registers the publisher's step as its own")
                    .isFalse();
            assertThat(outcome.publisherTerminal()).as("the publishing workflow completes too").isTrue();
            assertThat(outcome.requestRecords()).as("one published event, one record").isEqualTo(1);
            assertThat(outcome.stableAfterCrash())
                    .as("a crash after everything is durable changes no count")
                    .isTrue();
        }
    }

    @Nested
    class CrashWindows {

        @Test
        @Timeout(value = 90, unit = TimeUnit.SECONDS)
        void aCrashAfterThePublishedEventIsDurableNeverPublishesItAgain() {
            var outcome = PublishCrashScenario.crashAfterPublishCommitted(17L, "k1");
            logger.info("Crash after publish committed: {}", outcome);
            assertThat(outcome.requestRecordsAtCrash()).as("the event was durable when the crash hit").isEqualTo(1);
            assertThat(outcome.requestRecords())
                    .as("the recovered body finds the step in the sourced state and does not publish again")
                    .isEqualTo(1);
            assertThat(outcome.responderStarts()).as("the responder is started once, not once per run").isEqualTo(1);
            assertThat(outcome.observerStarts()).isEqualTo(1);
            assertThat(outcome.allTerminal()).isTrue();
        }

        @Test
        @Timeout(value = 90, unit = TimeUnit.SECONDS)
        void aVanishedPublishLeavesThePublisherParkedAndTheRecoveryPublishesExactlyOnce() {
            var outcome = PublishCrashScenario.publishVanishesThenCrash(19L, "k2");
            logger.info("Publish vanished then crash: {}", outcome);
            assertThat(outcome.vanishFired()).as("or this probe proves nothing").isTrue();
            assertThat(outcome.requestRecordsAtCrash()).as("nothing durable before the crash").isZero();
            assertThat(outcome.requestRecords())
                    .as("the effect of a publish is its record: nothing lost, nothing duplicated, one record after recovery")
                    .isEqualTo(1);
            assertThat(outcome.responderStarts()).isEqualTo(1);
            assertThat(outcome.allTerminal()).isTrue();
        }
    }

    @Nested
    class PlainAxonConsumer {

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        void aRegularEventHandlerReceivesEachPublishedEventOnceWithPayloadAndPublisherMetadata() {
            var outcome = PublishToEventHandlerScenario.run(23L, "h1");
            logger.info("Publish to plain handler: {}", outcome);
            assertThat(outcome.requestsReceived()).as("the plain handler receives the published request once").isEqualTo(1);
            assertThat(outcome.repliesReceived()).as("the plain handler receives the published reply once").isEqualTo(1);
            assertThat(outcome.requestPayloadOk()).as("the payload is the business event the workflow published").isTrue();
            assertThat(outcome.replyPayloadOk()).isTrue();
            assertThat(outcome.requestFromRequester()).as("the event carries the publisher's workflowId").isTrue();
            assertThat(outcome.replyFromResponder()).isTrue();
        }
    }
}
