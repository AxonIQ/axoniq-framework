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

package org.axonframework.messaging.core.sequencing;

import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventTestUtils;
import org.junit.jupiter.api.*;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link SequentialPolicy}.
 *
 * @author Allard Buijze
 */
class SequentialPolicyTest {

    @Test
    void sequencingIdentifier() {
        // ok, pretty useless, but everything should be tested
        SequentialPolicy testSubject = SequentialPolicy.INSTANCE;
        Object id1 = testSubject.sequenceIdentifierFor(EventTestUtils.asEventMessage(UUID.randomUUID()),
                                                          new StubProcessingContext()).orElse(null);
        Object id2 = testSubject.sequenceIdentifierFor(EventTestUtils.asEventMessage(UUID.randomUUID()),
                                                          new StubProcessingContext()).orElse(null);
        Object id3 = testSubject.sequenceIdentifierFor(EventTestUtils.asEventMessage(UUID.randomUUID()),
                                                          new StubProcessingContext()).orElse(null);

        assertEquals(id1, id2);
        assertEquals(id2, id3);
        // this can only fail if equals is not implemented correctly
        assertEquals(id1, id3);
    }
}
