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

package org.axonframework.messaging.unitofwork;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * @author Allard Buijze
 */
class CurrentUnitOfWorkTest {

    @BeforeEach
    void setUp() {
        while (CurrentUnitOfWork.isStarted()) {
            CurrentUnitOfWork.get().rollback();
        }
    }

    @AfterEach
    void tearDown() {
        while (CurrentUnitOfWork.isStarted()) {
            CurrentUnitOfWork.get().rollback();
        }
    }

    @Test
    void getSession_NoCurrentSession() {
        assertThrows(IllegalStateException.class, CurrentUnitOfWork::get);
    }

    @Test
    void setSession() {
        LegacyUnitOfWork<?> mockUnitOfWork = mock(LegacyUnitOfWork.class);
        CurrentUnitOfWork.set(mockUnitOfWork);
        assertSame(mockUnitOfWork, CurrentUnitOfWork.get());

        CurrentUnitOfWork.clear(mockUnitOfWork);
        assertFalse(CurrentUnitOfWork.isStarted());
    }

    @Test
    void notCurrentUnitOfWorkCommitted() {
        LegacyDefaultUnitOfWork<?> outerUoW = new LegacyDefaultUnitOfWork<>(null);
        outerUoW.start();
        new LegacyDefaultUnitOfWork<>(null).start();
        try {
            outerUoW.commit();
        } catch (IllegalStateException e) {
            return;
        }
        throw new AssertionError("The unit of work is not the current");
    }

}
