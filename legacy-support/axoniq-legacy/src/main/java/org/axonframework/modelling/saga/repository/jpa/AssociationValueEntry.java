/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package org.axonframework.modelling.saga.repository.jpa;

import jakarta.persistence.Basic;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import org.axonframework.modelling.saga.AssociationValue;

/**
 * JPA wrapper around an Association Value. This entity is used to store relevant Association Values for Sagas.
 *
 * @author Allard Buijze
 * @since 0.7
 */
@Table(indexes = {
        @Index(columnList = "sagaType, associationKey, associationValue", unique = false),
        @Index(columnList = "sagaId, sagaType", unique = false)
})
@Entity
public class AssociationValueEntry {

    @Id
    @GeneratedValue
    private Long id;

    @Basic(optional = false)
    private String sagaId;

    @Basic(optional = false)
    private String associationKey;

    @Basic
    private String associationValue;

    @Basic
    private String sagaType;

    /**
     * Initialize a new AssociationValueEntry for a saga with given {@code sagaIdentifier} and
     * {@code associationValue}.
     *
     * @param sagaType         The type of Saga this association value belongs to
     * @param sagaIdentifier   The identifier of the saga
     * @param associationValue The association value for the saga
     */
    public AssociationValueEntry(String sagaType, String sagaIdentifier, AssociationValue associationValue) {
        this.sagaType = sagaType;
        this.sagaId = sagaIdentifier;
        this.associationKey = associationValue.getKey();
        this.associationValue = associationValue.getValue();
    }

    /**
     * Constructor required by JPA. Do not use directly.
     */
    @SuppressWarnings({"UnusedDeclaration"})
    protected AssociationValueEntry() {
    }

    /**
     * Returns the association value contained in this entry.
     *
     * @return the association value contained in this entry
     */
    public AssociationValue getAssociationValue() {
        return new AssociationValue(associationKey, associationValue);
    }

    /**
     * Returns the Saga Identifier contained in this entry.
     *
     * @return the Saga Identifier contained in this entry
     */
    public String getSagaIdentifier() {
        return sagaId;
    }

    /**
     * Returns the type (fully qualified class name) of the Saga this association value belongs to
     *
     * @return the type (fully qualified class name) of the Saga
     */
    public String getSagaType() {
        return sagaType;
    }

    /**
     * The unique identifier of this entry.
     *
     * @return the unique identifier of this entry
     */
    public Long getId() {
        return id;
    }
}
