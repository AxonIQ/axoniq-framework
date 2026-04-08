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

package org.axonframework.messaging.commandhandling;

import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;

/**
 * Test utilities when dealing with {@link CommandBus}.
 *
 * @author Mateusz Nowak
 */
public final class CommandBusTestUtils {

    /**
     * Creates a new instance of {@link SimpleCommandBus} configured with a simple
     * {@link UnitOfWorkTestUtils#SIMPLE_FACTORY} and an empty list of processing lifecycle handler registrars.
     *
     * @return an instance of {@link SimpleCommandBus}
     */
    public static SimpleCommandBus aCommandBus() {
        return new SimpleCommandBus(UnitOfWorkTestUtils.SIMPLE_FACTORY);
    }

    private CommandBusTestUtils() {
        // Utility class
    }
}
