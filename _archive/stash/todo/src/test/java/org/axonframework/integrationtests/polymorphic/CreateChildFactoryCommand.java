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

/**
 * The creational command that creates either child one or child two based on {@link #child} field. The command handler
 * is placed on abstract parent aggregate.
 *
 * @author Milan Savic
 */
public class CreateChildFactoryCommand {

    private final String id;
    private final int child;

    public CreateChildFactoryCommand(String id, int child) {
        this.id = id;
        this.child = child;
    }

    public String getId() {
        return id;
    }

    public int getChild() {
        return child;
    }
}
