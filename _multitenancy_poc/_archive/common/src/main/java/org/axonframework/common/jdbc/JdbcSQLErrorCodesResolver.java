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

package org.axonframework.common.jdbc;

import java.sql.SQLIntegrityConstraintViolationException;

/**
 * {@link JdbcSQLErrorCodesResolver} is an implementation of {@link PersistenceExceptionResolver} used to resolve SQL
 * error codes to see if it is a duplicate key constraint violation.
 * <p/>
 *
 * @author Kristian Rosenvold
 * @since 2.2
 */
public class JdbcSQLErrorCodesResolver implements PersistenceExceptionResolver {

    @Override
    public boolean isDuplicateKeyViolation(Exception exception) {
        return causeIsEntityExistsException(exception);
    }

    private boolean causeIsEntityExistsException(Throwable exception) {
        return exception instanceof SQLIntegrityConstraintViolationException
                || (exception.getCause() != null && causeIsEntityExistsException(exception.getCause()));
    }

}
