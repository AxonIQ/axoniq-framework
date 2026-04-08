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

package org.axonframework.test.deadline;

import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.messaging.core.Scope;
import org.axonframework.messaging.core.ScopeDescriptor;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StubDeadlineManagerTest {

    private StubDeadlineManager testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new StubDeadlineManager();
    }

    @Disabled("TODO reintegrate as part of #3065")
    @Test
    void messagesCarryTriggerTimestamp() throws Exception {
        Instant triggerTime = Instant.now().plusSeconds(60);
        MockScope.execute(() ->
                                  testSubject.schedule(triggerTime, "gone")
        );
        List<DeadlineMessage> triggered = new ArrayList<>();
        testSubject.advanceTimeBy(Duration.ofMinutes(75), (s, message) -> triggered.add(message));

        assertEquals(1, triggered.size());
        assertEquals(triggerTime, triggered.get(0).timestamp());
    }

    private static class MockScope extends Scope {

        private static final MockScope instance = new MockScope();

        public static void execute(Runnable task) throws Exception {
            instance.executeWithResult(() -> {
                task.run();
                return null;
            });
        }

        @Override
        public ScopeDescriptor describeScope() {
            return (ScopeDescriptor) () -> "Mock";
        }
    }
}
