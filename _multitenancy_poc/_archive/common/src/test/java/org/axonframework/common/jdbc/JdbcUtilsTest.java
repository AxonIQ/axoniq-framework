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

import org.junit.jupiter.api.*;

import java.sql.ResultSet;

import static org.axonframework.common.jdbc.JdbcUtils.nextAndExtract;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests the {@link JdbcUtils} class static methods
 *
 * @author Albert Attard (Java Creed)
 * @see JdbcUtils
 */
class JdbcUtilsTest {

    /**
     * Tries to read from an empty result set. The method is expected to return {@code null}
     *
     * @see JdbcUtils#nextAndExtract(ResultSet, int, Class)
     * @see JdbcUtils#extract(ResultSet, int, Class)
     */
    @Test
    void nextAndExtract_EmptyResultSet() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);

        when(resultSet.next()).thenReturn(false);
        assertNull(nextAndExtract(resultSet, 1, Long.class));
    }

    /**
     * Reads {@code null} from the result set. The result set here returns {@code null}.
     *
     * @see JdbcUtils#nextAndExtract(ResultSet, int, Class)
     * @see JdbcUtils#extract(ResultSet, int, Class)
     */
    @Test
    void nextAndExtract_NullValue() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);

        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(1, Long.class)).thenReturn(null);
        assertNull(nextAndExtract(resultSet, 1, Long.class));
    }

    @Test
    void nextAndExtractWithDefault_DefaultValue() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        Long defaultValue = 42L;

        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(1, Long.class)).thenReturn(null);
        assertEquals(defaultValue, nextAndExtract(resultSet, 1, Long.class, defaultValue));
    }

    /**
     * Reads a value from the results set. The method should return the value that was read.
     *
     * @see JdbcUtils#nextAndExtract(ResultSet, int, Class)
     * @see JdbcUtils#extract(ResultSet, int, Class)
     */
    @Test
    void nextAndExtract_NonNullValue() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);

        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(1, Long.class)).thenReturn(10L);
        assertEquals(Long.valueOf(10L), nextAndExtract(resultSet, 1, Long.class));
    }

    /**
     * Reads a null value but the result set returns 0L while the wasNull() method returns false, which indicates that
     * the value read from the result set was actually {@code null}.
     * <p>
     * This test was added to address issue
     * <a href="https://github.com/AxonFramework/AxonFramework/issues/636">#638</a>
     *
     * @see JdbcUtils#nextAndExtract(ResultSet, int, Class)
     * @see JdbcUtils#extract(ResultSet, int, Class)
     */
    @Test
    void nextAndExtract_NonNullValue_WasNull() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);

        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(1, Long.class)).thenReturn(0L);
        when(resultSet.wasNull()).thenReturn(true);
        assertNull(nextAndExtract(resultSet, 1, Long.class));
    }

    @Test
    void nextAndExtractWithDefault_NonNullValue_WasDefault() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        Long defaultValue = 42L;

        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(1, Long.class)).thenReturn(0L);
        when(resultSet.wasNull()).thenReturn(true);
        assertEquals(defaultValue, nextAndExtract(resultSet, 1, Long.class, defaultValue));
    }
}
