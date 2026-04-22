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

package io.axoniq.framework.messaging.eventhandling.deadletter.jdbc;

import org.axonframework.messaging.eventhandling.EventMessage;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A functional interface describing how to convert a {@link ResultSet} in {@link JdbcDeadLetter} implementation of type
 * {@code D}
 *
 * @param <E> An implementation of {@link EventMessage} contained within the {@link JdbcDeadLetter} implementation this
 *            converter converts.
 * @param <D> An implementation of {@link JdbcDeadLetter} converted by this converter.
 * @author Steven van Beelen
 * @since 4.8.0
 */
@FunctionalInterface
public interface DeadLetterJdbcConverter<E extends EventMessage, D extends JdbcDeadLetter<E>> {

    /**
     * Converts the given {@code resultSet} in an implementation of {@link JdbcDeadLetter}.
     * <p>
     * It is recommended to validate the type of {@link EventMessage} to place in the result, as different types require
     * additional information to be deserialized and returned.
     *
     * @param resultSet The {@link ResultSet} to convert into a {@link JdbcDeadLetter}
     * @return An implementation of {@link JdbcDeadLetter} based on the given {@code resultSet}.
     * @throws SQLException if the a {@code columnLabel} in the given {@code resultSet} does not exist, if a database
     *                      access error occurs or if the given {@code resultSet} is closed.
     */
    D convertToLetter(ResultSet resultSet) throws SQLException;
}
