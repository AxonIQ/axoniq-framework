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

import java.sql.Connection;
import java.sql.SQLException;

import static org.axonframework.common.jdbc.ConnectionWrapperFactory.wrap;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link ConnectionWrapperFactory}.
 *
 * @author Allard Buijze
 */
@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
class ConnectionWrapperFactoryTest {

    private ConnectionWrapperFactory.ConnectionCloseHandler closeHandler;
    private Connection connection;

    @BeforeEach
    void setUp() {
        closeHandler = mock(ConnectionWrapperFactory.ConnectionCloseHandler.class);
        connection = mock(Connection.class);
    }

    @Test
    void wrapperDelegatesAllButClose() throws Exception {
        Connection wrapped = wrap(connection, closeHandler);
        wrapped.commit();
        verify(closeHandler).commit(connection);

        wrapped.getAutoCommit();
        verify(connection).getAutoCommit();

        verifyNoMoreInteractions(closeHandler);

        wrapped.close();
        verify(connection, never()).close();
        verify(closeHandler).close(connection);
    }

    @Test
    void equals_WithWrapper() {
        final Runnable runnable = mock(Runnable.class);
        Connection wrapped = wrap(connection, Runnable.class, runnable, closeHandler);

        assertNotEquals(wrapped, connection);
        assertEquals(wrapped, wrapped);
    }

    @Test
    void equals_WithoutWrapper() {
        Connection wrapped = wrap(connection, closeHandler);

        assertNotEquals(wrapped, connection);
        assertEquals(wrapped, wrapped);
    }

    @Test
    void hashCode_WithWrapper() {
        final Runnable runnable = mock(Runnable.class);
        Connection wrapped = wrap(connection, Runnable.class, runnable, closeHandler);
        assertEquals(wrapped.hashCode(), wrapped.hashCode());
    }

    @Test
    void hashCode_WithoutWrapper() {
        Connection wrapped = wrap(connection, closeHandler);
        assertEquals(wrapped.hashCode(), wrapped.hashCode());
    }

    @Test
    void unwrapInvocationTargetException() throws Exception {
        when(connection.prepareStatement(anyString())).thenThrow(new SQLException());

        Connection wrapper = wrap(connection, closeHandler);
        assertThrows(SQLException.class, () -> wrapper.prepareStatement("foo"));
    }

    @Test
    void unwrapInvocationTargetExceptionWithAdditionalWrapperInterface1() throws Exception {
        WrapperInterface wrapperImplementation = mock(WrapperInterface.class);
        when(connection.prepareStatement(anyString())).thenThrow(new SQLException());

        Connection wrapper = wrap(connection, WrapperInterface.class, wrapperImplementation, closeHandler);
        assertThrows(SQLException.class, () -> wrapper.prepareStatement("foo"));
    }

    @Test
    void unwrapInvocationTargetExceptionWithAdditionalWrapperInterface2() throws Exception {
        WrapperInterface wrapperImplementation = mock(WrapperInterface.class);
        doThrow(new SQLException()).when(wrapperImplementation).foo();

        WrapperInterface wrapper = (WrapperInterface) wrap(connection,
                                                           WrapperInterface.class,
                                                           wrapperImplementation,
                                                           closeHandler);
        assertThrows(SQLException.class, wrapper::foo);
    }

    private interface WrapperInterface {

        void foo() throws SQLException;
    }
}
