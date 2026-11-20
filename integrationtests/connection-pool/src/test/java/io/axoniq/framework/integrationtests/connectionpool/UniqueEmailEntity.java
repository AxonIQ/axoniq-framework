/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * The application's own table, mapped so that a JPA persistence unit has something to manage.
 *
 * @author John Hendrikx
 */
@Entity
@Table(name = "unique_email")
class UniqueEmailEntity {

    @Id
    private String email;

    /**
     * Constructs a new instance, as required by JPA.
     */
    protected UniqueEmailEntity() {
    }
}
