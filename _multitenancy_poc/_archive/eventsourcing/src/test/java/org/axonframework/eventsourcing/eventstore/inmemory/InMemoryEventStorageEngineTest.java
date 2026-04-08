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

package org.axonframework.eventsourcing.eventstore.inmemory;

import org.axonframework.eventsourcing.eventstore.StorageEngineTestSuite;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Test class validating the {@link InMemoryEventStorageEngine}.
 *
 * @author Steven van Beelen
 */
class InMemoryEventStorageEngineTest extends StorageEngineTestSuite<InMemoryEventStorageEngine> {

    @Override
    protected InMemoryEventStorageEngine createStorageEngine() {
        return new InMemoryEventStorageEngine();
    }

    @Override
    protected ProcessingContext processingContext() {
        return null;
    }

    @Override  // disable this unsupported scenario
    protected void twoIndependentStorageEnginesShouldSeeEachOthersAppends() {
    }
}
