/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.eventsourcing.snapshot.store;

import org.axonframework.eventsourcing.snapshot.api.EvolutionResult;
import org.axonframework.eventsourcing.snapshot.api.SnapshotPolicy;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link SnapshotPolicy}.
 *
 * @author John Hendrikx
 */
class SnapshotPolicyTest {

    @Test
    void afterEventsPolicyShouldRejectInvalidParameters() {
        assertThatThrownBy(() -> SnapshotPolicy.afterEvents(-1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SnapshotPolicy.afterEvents(Integer.MIN_VALUE)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void whenEventMatchesPolicyShouldRejectInvalidParameters() {
        assertThatThrownBy(() -> SnapshotPolicy.whenEventMatches(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void afterEventsPolicyShouldWorkWithValidParameters() {
        SnapshotPolicy policy = SnapshotPolicy.afterEvents(5);

        assertThat(policy.shouldSnapshot(new EvolutionResult(6, Duration.ZERO))).isTrue();
        assertThat(policy.shouldSnapshot(new EvolutionResult(5, Duration.ZERO))).isFalse();
        assertThat(policy.shouldSnapshot(new EvolutionResult(5, Duration.ofDays(1)))).isFalse();
    }

    @Test
    void whenSourcingTimeExceedsPolicyShouldRejectInvalidParameters() {
        assertThatThrownBy(() -> SnapshotPolicy.whenSourcingTimeExceeds(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void whenSourcingTimeExceedsPolicyShouldWorkWithValidParameters() {
        SnapshotPolicy policy = SnapshotPolicy.whenSourcingTimeExceeds(Duration.ofSeconds(1));

        assertThat(policy.shouldSnapshot(new EvolutionResult(0, Duration.ofMillis(1001)))).isTrue();
        assertThat(policy.shouldSnapshot(new EvolutionResult(0, Duration.ofMillis(1000)))).isFalse();
        assertThat(policy.shouldSnapshot(new EvolutionResult(1000, Duration.ofMillis(1000)))).isFalse();
    }

    @Test
    void whenRequestedPolicyShouldWork() {
        SnapshotPolicy policy = SnapshotPolicy.whenEventMatches(msg -> msg.payload() instanceof String s && s.equals("Hi"));

        assertThat(policy.shouldSnapshot(new GenericEventMessage(MessageType.fromString("a#1"), "Hi"))).isTrue();
        assertThat(policy.shouldSnapshot(new GenericEventMessage(MessageType.fromString("a#1"), "Bye"))).isFalse();
    }
}
