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

package io.axoniq.workflow.runtime.util;

import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.TransactionalUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.Transaction;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.junit.jupiter.api.*;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;

/**
 * Unit test class for the {@code ProcessingContextUtils} utility class.
 * @author Simon Zambrovski
 */
class ProcessingContextUtilsTest {

    private UnitOfWorkFactory unitOfWorkFactory;
    private ProcessingContext parentContext;
    private ProcessingContext childContext;
    private Executor executor;

    @BeforeEach
    void setUp() {
        unitOfWorkFactory = mock(UnitOfWorkFactory.class);
        parentContext = mock(ProcessingContext.class);
        childContext = mock(ProcessingContext.class);
        UnitOfWork unitOfWork = mock(UnitOfWork.class);
        executor = mock(Executor.class);

        when(unitOfWorkFactory.create(any(Function.class))).thenReturn(unitOfWork);
        when(unitOfWorkFactory.create(anyString(), any(Function.class))).thenReturn(unitOfWork);

        // Mock executeWithResult on UnitOfWork
        when(unitOfWork.executeWithResult(any(Function.class))).thenAnswer(invocation -> {
            Function<ProcessingContext, CompletableFuture<?>> action = invocation.getArgument(0);
            return action.apply(childContext);
        });

        // Mock resources on parentContext
        Map<Context.ResourceKey<?>, Object> resources = new HashMap<>();
        // In Axon, ResourceKey is usually just a wrapper, or we can use any object if we mock it right.
        // Let's use a mock for ResourceKey
        Context.ResourceKey<String> key = mock(Context.ResourceKey.class);
        resources.put(key, "testValue");
        when(parentContext.resources()).thenReturn(resources);
    }

    @Test
    void testConstructorIsPrivate() throws NoSuchMethodException {
        Constructor<ProcessingContextUtils> constructor = ProcessingContextUtils.class.getDeclaredConstructor();
        assertThat(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers())).isTrue();
        constructor.setAccessible(true);
        assertThatCode(constructor::newInstance).doesNotThrowAnyException();
    }

    @Test
    void testCopyResources() {
        ProcessingContext result = ProcessingContextUtils.copyResources(parentContext, childContext);
        assertThat(result).isEqualTo(childContext);
        verify(childContext).putResourceIfAbsent(any(), eq("testValue"));
    }

    @Test
    void executeWithResultKeepsTheConnectionInstalledByTheChildTransactionManager() {
        var key = Context.ResourceKey.<Connection>withLabel("connection");
        var parentConnection = mock(Connection.class);
        var childConnection = mock(Connection.class);
        var parent = Context.with(key, parentConnection);
        var transactionManager = new ConnectionInstallingTransactionManager(key, childConnection);
        var transactionalFactory = new TransactionalUnitOfWorkFactory(
                transactionManager,
                new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE)
        );

        var result = ProcessingContextUtils.executeWithResult(
                "child",
                transactionalFactory,
                Runnable::run,
                parent,
                context -> {
                    assertThat(context.getResource(key)).isSameAs(childConnection);
                    return CompletableFuture.completedFuture(null);
                }
        );

        assertThat(result.join()).isNull();
    }

    @Test
    void testCopyResourcesAddsTheResourcesTheTargetLacks() {
        var key = Context.ResourceKey.<String>withLabel("connection");
        var from = Context.with(key, "parentConnection");

        new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE)
                .create()
                .executeWithResult(target -> {
                    ProcessingContextUtils.copyResources(from, target);

                    assertThat(target.<String>getResource(key)).isEqualTo("parentConnection");
                    return CompletableFuture.completedFuture(null);
                })
                .join();
    }

    @Test
    void testExecuteWithResultNoId() {
        Function<ProcessingContext, CompletableFuture<String>> action = ctx -> CompletableFuture.completedFuture("completion");

        CompletableFuture<String> resultFuture = ProcessingContextUtils.executeWithResult(
                null, unitOfWorkFactory, executor, parentContext, action
        );

        assertThat(resultFuture.join()).isEqualTo("completion");
        verify(unitOfWorkFactory).create(any(Function.class));
        verify(childContext).putResourceIfAbsent(any(), eq("testValue"));
        // verify lifecycle hooks registration
        verify(childContext).whenComplete(any());
        verify(childContext).onAfterCommit(any());
        verify(childContext).onPrepareCommit(any());
    }

    @Test
    void testExecuteWithResultWithId() {
        Function<ProcessingContext, CompletableFuture<String>> action = ctx -> CompletableFuture.completedFuture("completion");

        CompletableFuture<String> resultFuture = ProcessingContextUtils.executeWithResult(
                "myId", unitOfWorkFactory, executor, parentContext, action
        );

        assertThat(resultFuture.join()).isEqualTo("completion");
        verify(unitOfWorkFactory).create(anyString(), any(Function.class));
    }

    @Test
    void testExecuteWithResultInSeparateThread() {
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).execute(any(Runnable.class));

        Function<ProcessingContext, CompletableFuture<String>> action = ctx -> CompletableFuture.completedFuture("completion");

        var result = ProcessingContextUtils.executeWithResultInSeparateThread(
                "myId", unitOfWorkFactory, executorService, parentContext, action
        );

        assertThat(result.join()).isEqualTo("completion");
        verify(executorService).execute(any(Runnable.class));
        verify(unitOfWorkFactory).create(anyString(), any(Function.class));
    }

    @Test
    void executeWithResultInSeparateThreadDoesNotBlockItsExecutorWhileTheBodyIsParked() throws Exception {
        var bodyStarted = new CountDownLatch(1);
        var parkedBody = new CompletableFuture<String>();
        try (var executorService = Executors.newSingleThreadExecutor()) {
            var result = ProcessingContextUtils.executeWithResultInSeparateThread(
                    "myId",
                    unitOfWorkFactory,
                    executorService,
                    parentContext,
                    context -> {
                        bodyStarted.countDown();
                        return parkedBody;
                    }
            );

            assertThat(bodyStarted.await(5, TimeUnit.SECONDS)).isTrue();
            executorService.shutdown();
            assertThat(executorService.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(result).isNotDone();

            parkedBody.complete("completion");
            assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo("completion");
        }
    }

    private static class ConnectionInstallingTransactionManager implements TransactionManager {

        private final Context.ResourceKey<Connection> key;
        private final Connection connection;

        private ConnectionInstallingTransactionManager(Context.ResourceKey<Connection> key, Connection connection) {
            this.key = key;
            this.connection = connection;
        }

        @Override
        public Transaction startTransaction() {
            return mock(Transaction.class);
        }

        @Override
        public void attachToProcessingLifecycle(ProcessingLifecycle lifecycle) {
            lifecycle.runOnPreInvocation(context -> context.putResource(key, connection));
        }
    }
}
