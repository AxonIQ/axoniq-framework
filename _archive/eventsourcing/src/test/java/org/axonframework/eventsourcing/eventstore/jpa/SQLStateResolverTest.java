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

package org.axonframework.eventsourcing.eventstore.jpa;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Jochen Munz
 */
class SQLStateResolverTest {

    @Test
    void defaultResolver_duplicateKeyException() {
        SQLStateResolver resolver = new SQLStateResolver();

        boolean isDuplicateKey = resolver.isDuplicateKeyViolation(duplicateKeyException());

        assertTrue(isDuplicateKey);
    }

    @Test
    void defaultResolver_integrityConstraintViolated() {
        SQLStateResolver resolver = new SQLStateResolver();

        boolean isDuplicateKey = resolver.isDuplicateKeyViolation(integrityContraintViolation());

        assertTrue(isDuplicateKey);
    }

    @Test
    void explicitResolver_duplicateKeyException() {
        SQLStateResolver resolver = new SQLStateResolver("23505");

        boolean isDuplicateKey = resolver.isDuplicateKeyViolation(duplicateKeyException());

        assertTrue(isDuplicateKey);
    }


    @Test
    void explicitResolver_integrityConstraintViolated() {
        SQLStateResolver resolver = new SQLStateResolver("23505");

        boolean isDuplicateKey = resolver.isDuplicateKeyViolation(integrityContraintViolation());

        assertFalse(isDuplicateKey, "A general state code should not be matched by the explicitly configured resolver");
    }

    private Exception integrityContraintViolation() {
        return new SQLException("general state code", "23000");
    }


    private Exception duplicateKeyException() {
        return new SQLException("detailed state code", "23505");
    }

}