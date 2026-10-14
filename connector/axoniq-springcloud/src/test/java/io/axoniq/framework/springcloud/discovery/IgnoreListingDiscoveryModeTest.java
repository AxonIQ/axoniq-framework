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

package io.axoniq.framework.springcloud.discovery;

import io.axoniq.framework.springcloud.routing.MemberCapabilities;
import io.axoniq.framework.springcloud.util.TestServiceInstance;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;
import org.springframework.cloud.client.ServiceInstance;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how {@link IgnoreListingDiscoveryMode} keeps instances that do not serve capabilities out of the way.
 *
 * @author Allard Buijze
 * @author Steven van Beelen
 */
class IgnoreListingDiscoveryModeTest {

    private static final Duration IGNORE_PERIOD = Duration.ofMinutes(1);
    private static final MemberCapabilities CAPABILITIES = new MemberCapabilities(
            100, Set.of(new QualifiedName("university.CreateCourse")), Set.of()
    );

    private TestServiceInstance axonNode;
    private TestServiceInstance unrelatedService;
    private RecordingCapabilityDiscoveryMode delegate;
    private MutableClock clock;
    private IgnoreListingDiscoveryMode testSubject;

    @BeforeEach
    void setUp() {
        axonNode = TestServiceInstance.instance("university", "node-a", 8080);
        unrelatedService = TestServiceInstance.instance("billing", "node-z", 9090);
        delegate = new RecordingCapabilityDiscoveryMode()
                .answering(axonNode, CAPABILITIES)
                .failingWithClientError(unrelatedService);
        clock = new MutableClock(Instant.parse("2026-08-28T10:00:00Z"));
        testSubject = new IgnoreListingDiscoveryMode(delegate, IGNORE_PERIOD, clock);
    }

    @Nested
    class Delegating {

        @Test
        void reportsTheCapabilitiesTheDelegateFound() {
            // when
            Optional<MemberCapabilities> capabilities = capabilitiesOf(axonNode);

            // then
            assertThat(capabilities).contains(CAPABILITIES);
        }

        @Test
        void passesLocalCapabilitiesToTheDelegate() {
            // when
            testSubject.updateLocalCapabilities(CAPABILITIES);

            // then
            assertThat(delegate.localCapabilities()).isEqualTo(CAPABILITIES);
            assertThat(testSubject.localCapabilities()).isEqualTo(CAPABILITIES);
        }

        @Test
        void identifiesThisApplicationByTheNodeIdOfTheDelegate() {
            // when / then
            assertThat(testSubject.localNodeId()).isEqualTo(delegate.localNodeId());
        }

        @Test
        void passesTheRetainedInstancesToTheDelegate() {
            // when
            testSubject.retainOnly(Set.of(ServiceInstanceKey.of(axonNode)));

            // then
            assertThat(delegate.retained()).containsExactly(Set.of(ServiceInstanceKey.of(axonNode)));
        }
    }

    @Nested
    class Ignoring {

        @Test
        void reportsAnInstanceAnsweringWithAClientErrorAsNotPartOfTheCluster() {
            // when
            Optional<MemberCapabilities> capabilities = capabilitiesOf(unrelatedService);

            // then — an empty result leaves it out of the ring entirely, unlike INCAPABLE which keeps it in
            assertThat(capabilities).isEmpty();
        }

        @Test
        void stopsAskingAnInstanceThatAnsweredWithAClientError() {
            // given
            capabilitiesOf(unrelatedService);
            int askedOnce = delegate.asked().size();

            // when
            capabilitiesOf(unrelatedService);
            capabilitiesOf(unrelatedService);

            // then — the point of the ignore list: an unrelated service is not queried on every heartbeat
            assertThat(delegate.asked()).hasSize(askedOnce);
        }

        @Test
        void goesOnAskingTheOtherInstances() {
            // given
            capabilitiesOf(unrelatedService);

            // when
            Optional<MemberCapabilities> capabilities = capabilitiesOf(axonNode);

            // then
            assertThat(capabilities).contains(CAPABILITIES);
        }

        @Test
        void asksAnIgnoredInstanceAgainOnceTheThresholdHasPassed() {
            // given — the instance answered with a client error only because it was still starting up, and has
            // since come up
            capabilitiesOf(unrelatedService);
            delegate.recovering(unrelatedService, CAPABILITIES);

            // when
            clock.advance(IGNORE_PERIOD.plusSeconds(1));

            // then — a permanent ignore list would keep a recovering instance out of the cluster for good
            assertThat(capabilitiesOf(unrelatedService)).contains(CAPABILITIES);
        }

        @Test
        void keepsIgnoringUntilTheThresholdHasPassed()  {
            // given
            capabilitiesOf(unrelatedService);
            int askedOnce = delegate.asked().size();

            // when
            clock.advance(IGNORE_PERIOD.minusSeconds(1));
            capabilitiesOf(unrelatedService);

            // then
            assertThat(delegate.asked()).hasSize(askedOnce);
        }

        @Test
        void forgetsIgnoredInstancesThatAreNoLongerReported() {
            // given
            capabilitiesOf(unrelatedService);

            // when — discovery no longer reports the unrelated service at all
            testSubject.retainOnly(Set.of(ServiceInstanceKey.of(axonNode)));
            delegate.answering(unrelatedService, CAPABILITIES);
            capabilitiesOf(unrelatedService);

            // then — the entry was discarded with the instance, so it is asked again straight away
            assertThat(delegate.asked()).contains(ServiceInstanceKey.of(unrelatedService));
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsANullDelegate() {
            // when / then
            assertThatThrownBy(() -> new IgnoreListingDiscoveryMode(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        void rejectsANonPositiveIgnorePeriod() {
            // when / then
            assertThatThrownBy(() -> new IgnoreListingDiscoveryMode(delegate, Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("strictly positive");
            assertThatThrownBy(() -> new IgnoreListingDiscoveryMode(delegate, Duration.ofMinutes(-1)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private Optional<MemberCapabilities> capabilitiesOf(ServiceInstance instance) {
        return testSubject.discover(instance).map(MemberAdvertisement::capabilities);
    }

    /**
     * A {@link Clock} a test can move forward, so expiry can be exercised without waiting for it.
     */
    private static class MutableClock extends Clock {

        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        private void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public java.time.ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
