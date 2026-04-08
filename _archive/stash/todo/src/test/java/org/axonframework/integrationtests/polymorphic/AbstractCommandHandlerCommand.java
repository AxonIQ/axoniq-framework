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

package org.axonframework.integrationtests.polymorphic;

import org.axonframework.modelling.command.TargetAggregateIdentifier;

/**
 * The command sent to the abstract command handler of parent aggregate in polymorphic aggregate hierarchy.
 *
 * @author Milan Savic
 */
public class AbstractCommandHandlerCommand {

    @TargetAggregateIdentifier
    private final String id;

    public AbstractCommandHandlerCommand(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
