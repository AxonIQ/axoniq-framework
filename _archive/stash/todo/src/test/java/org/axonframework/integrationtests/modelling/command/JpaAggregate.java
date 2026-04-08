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

package org.axonframework.integrationtests.modelling.command;

import jakarta.persistence.Basic;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Version;
import org.axonframework.common.IdentifierFactory;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.modelling.command.AggregateLifecycle;
import org.axonframework.modelling.command.AggregateVersion;
import org.axonframework.common.util.StubDomainEvent;

@Entity
public class JpaAggregate {

    @AggregateIdentifier
    @Id
    private String id;

    @AggregateVersion
    @Version
    private Long version;

    @Basic
    private String message;

    public JpaAggregate(String message) {
        this.id = IdentifierFactory.getInstance().generateIdentifier();
        this.message = message;
    }

    /**
     * Constructor performing very basic initialization, as required by JPA.
     */
    protected JpaAggregate() {
    }

    public void setMessage(String newMessage) {
        this.message = newMessage;
        AggregateLifecycle.apply(new StubDomainEvent(message));
    }

    public void delete() {
        AggregateLifecycle.markDeleted();
    }

    public String getIdentifier() {
        return id;
    }

    public Long getVersion() {
        return version;
    }
}
