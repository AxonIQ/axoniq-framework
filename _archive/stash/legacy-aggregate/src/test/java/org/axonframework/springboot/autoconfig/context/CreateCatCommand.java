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

package org.axonframework.springboot.autoconfig.context;

import org.axonframework.modelling.command.TargetAggregateIdentifier;

import java.util.Objects;

public class CreateCatCommand {

    @TargetAggregateIdentifier
    private final String aggregateId;
    private final String name;

    public CreateCatCommand(String aggregateId, String name) {
        this.aggregateId = aggregateId;
        this.name = name;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CreateCatCommand that = (CreateCatCommand) o;
        return Objects.equals(aggregateId, that.aggregateId) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aggregateId, name);
    }

    @Override
    public String toString() {
        return "CreateCatCommand{" + "aggregateId='" + aggregateId + '\'' + ", name='" + name + '\'' + '}';
    }
}
