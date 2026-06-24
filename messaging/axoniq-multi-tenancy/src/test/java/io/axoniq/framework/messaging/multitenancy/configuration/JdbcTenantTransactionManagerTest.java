/*
 * Copyright (c) 2010-2026. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.transaction.jdbc.JdbcTenantTransactionManager;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle.ErrorHandler;
import org.axonframework.messaging.core.unitofwork.transaction.jdbc.JdbcTransactionalExecutorProvider;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStore;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStoreConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.TokenSchema;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class JdbcTenantTransactionManagerTest {

    private static final JdbcTokenStoreConfiguration TOKEN_STORE_CONFIGURATION = JdbcTokenStoreConfiguration.DEFAULT
            .schema(TokenSchema.builder().setTokenTable("token_entry").build());

    @Test
    void attachesConnectionExecutorAndAllowsTokenStoreStartup() {
        DataSource dataSource = tenantDataSource("foo-a");
        JdbcTenantTransactionManager transactionManager = new JdbcTenantTransactionManager(dataSource);
        JdbcTokenStore tokenStore = new JdbcTokenStore(
                new JdbcTransactionalExecutorProvider(dataSource),
                new JacksonConverter(),
                TOKEN_STORE_CONFIGURATION
        );

        ProcessingLifecycle lifecycle = mock(ProcessingLifecycle.class);
        ArgumentCaptor<Consumer<ProcessingContext>> preInvocationCaptor = ArgumentCaptor.forClass(Consumer.class);

        transactionManager.attachToProcessingLifecycle(lifecycle);

        verify(lifecycle).runOnPreInvocation(preInvocationCaptor.capture());

        MutableProcessingContext processingContext = new MutableProcessingContext();
        preInvocationCaptor.getValue().accept(processingContext);

        assertThat(processingContext.containsResource(JdbcTransactionalExecutorProvider.SUPPLIER_KEY)).isTrue();
        assertThat(tokenEntryCount(dataSource)).isZero();

        tokenStore.initializeTokenSegments(
                          "Projection_CourseStats_Processor",
                          1,
                          new GlobalSequenceTrackingToken(0),
                          processingContext
                  )
                  .join();
        assertThat(tokenEntryCount(dataSource)).isZero();

        processingContext.commit();

        assertThat(tokenEntryCount(dataSource)).isEqualTo(1);
    }

    @Test
    void rollsBackTokenStoreChangesOnError() {
        DataSource dataSource = tenantDataSource("foo-b");
        JdbcTenantTransactionManager transactionManager = new JdbcTenantTransactionManager(dataSource);
        JdbcTokenStore tokenStore = new JdbcTokenStore(
                new JdbcTransactionalExecutorProvider(dataSource),
                new JacksonConverter(),
                TOKEN_STORE_CONFIGURATION
        );

        ProcessingLifecycle lifecycle = mock(ProcessingLifecycle.class);
        ArgumentCaptor<Consumer<ProcessingContext>> preInvocationCaptor = ArgumentCaptor.forClass(Consumer.class);

        transactionManager.attachToProcessingLifecycle(lifecycle);

        verify(lifecycle).runOnPreInvocation(preInvocationCaptor.capture());

        MutableProcessingContext processingContext = new MutableProcessingContext();
        preInvocationCaptor.getValue().accept(processingContext);

        assertThat(processingContext.containsResource(JdbcTransactionalExecutorProvider.SUPPLIER_KEY)).isTrue();

        tokenStore.initializeTokenSegments(
                          "Projection_CourseStats_Processor",
                          1,
                          new GlobalSequenceTrackingToken(0),
                          processingContext
                  )
                  .join();
        assertThat(tokenEntryCount(dataSource)).isZero();

        processingContext.fail(new IllegalStateException("boom"));

        assertThat(tokenEntryCount(dataSource)).isZero();
    }

    @Test
    void requiresSameThreadInvocationsReturnsTrue() {
        assertThat(new JdbcTenantTransactionManager(tenantDataSource("foo-c"))
                           .requiresSameThreadInvocations()).isTrue();
    }

    private DataSource tenantDataSource(String tenantId) {
        try {
            Path databaseFile = tenantDatabaseFile(tenantId);
            Files.createDirectories(databaseFile.getParent());

            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:file:" + databaseFile.toAbsolutePath().toString().replace('\\', '/')
                              + ";AUTO_SERVER=FALSE;DB_CLOSE_DELAY=-1");
            dataSource.setUser("sa");
            dataSource.setPassword("");
            initializeTenantSchema(dataSource);
            return dataSource;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create tenant data source", e);
        }
    }

    private static Path tenantDatabaseFile(String tenantId) {
        return Path.of("target", "h2-test", tenantId, String.valueOf(System.nanoTime()), "course-stats");
    }

    private static void initializeTenantSchema(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS token_entry (
                        processorName VARCHAR(255) NOT NULL,
                        segment INTEGER NOT NULL,
                        mask INTEGER NOT NULL,
                        token BLOB NULL,
                        tokenType VARCHAR(255) NULL,
                        timestamp VARCHAR(255) NULL,
                        owner VARCHAR(255) NULL,
                        PRIMARY KEY (processorName, segment)
                    )
                    """);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize tenant schema", e);
        }
    }

    private static long tokenEntryCount(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM token_entry")) {
            resultSet.next();
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to inspect token entry table", e);
        }
    }

    private static final class MutableProcessingContext implements ProcessingContext {

        private final Map<ResourceKey<?>, Object> resources = new HashMap<>();
        private Consumer<ProcessingContext> commitAction;
        private ErrorHandler errorHandler;

        @Override
        public boolean isStarted() {
            return false;
        }

        @Override
        public boolean isError() {
            return false;
        }

        @Override
        public boolean isCommitted() {
            return false;
        }

        @Override
        public boolean isCompleted() {
            return false;
        }

        @Override
        public ProcessingLifecycle on(ProcessingLifecycle.Phase phase,
                                      java.util.function.Function<ProcessingContext, java.util.concurrent.CompletableFuture<?>> action) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProcessingLifecycle runOnCommit(Consumer<ProcessingContext> action) {
            this.commitAction = action;
            return this;
        }

        @Override
        public ProcessingLifecycle onError(ErrorHandler action) {
            this.errorHandler = action;
            return this;
        }

        @Override
        public ProcessingLifecycle whenComplete(Consumer<ProcessingContext> action) {
            throw new UnsupportedOperationException();
        }

        void commit() {
            if (commitAction == null) {
                throw new IllegalStateException("No commit action registered");
            }
            commitAction.accept(this);
        }

        void fail(Throwable throwable) {
            if (errorHandler == null) {
                throw new IllegalStateException("No error handler registered");
            }
            errorHandler.handle(this, mock(ProcessingLifecycle.Phase.class), throwable);
        }

        @Override
        public boolean containsResource(ResourceKey<?> key) {
            return resources.containsKey(key);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T getResource(ResourceKey<T> key) {
            return (T) resources.get(key);
        }

        @Override
        public <T> ProcessingContext withResource(ResourceKey<T> key, T resource) {
            resources.put(key, resource);
            return this;
        }

        @Override
        public Map<ResourceKey<?>, Object> resources() {
            return Map.copyOf(resources);
        }

        @Override
        public <T> T putResource(ResourceKey<T> key, T resource) {
            @SuppressWarnings("unchecked")
            T previous = (T) resources.put(key, resource);
            return previous;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T updateResource(ResourceKey<T> key, java.util.function.UnaryOperator<T> resourceUpdater) {
            T updated = resourceUpdater.apply((T) resources.get(key));
            if (updated == null) {
                resources.remove(key);
            } else {
                resources.put(key, updated);
            }
            return updated;
        }

        @Override
        public <T> T putResourceIfAbsent(ResourceKey<T> key, T resource) {
            @SuppressWarnings("unchecked")
            T existing = (T) resources.putIfAbsent(key, resource);
            return existing;
        }

        @Override
        public <T> T computeResourceIfAbsent(ResourceKey<T> key, java.util.function.Supplier<T> resourceSupplier) {
            @SuppressWarnings("unchecked")
            T value = (T) resources.computeIfAbsent(key, unused -> resourceSupplier.get());
            return value;
        }

        @Override
        public <T> T removeResource(ResourceKey<T> key) {
            @SuppressWarnings("unchecked")
            T removed = (T) resources.remove(key);
            return removed;
        }

        @Override
        public <T> boolean removeResource(ResourceKey<T> key, T expectedResource) {
            return resources.remove(key, expectedResource);
        }

        @Override
        public <C> C component(Class<C> type, String name) {
            throw new UnsupportedOperationException();
        }
    }
}
