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

package io.axoniq.workflow.runtime.util;

import io.axoniq.workflow.runtime.util.ProcessingContextUtils;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWork;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit test class for the {@code ProcessingContextUtils} utility class.
 * @since 1.0.0
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
        assertTrue(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        assertDoesNotThrow(() -> { constructor.newInstance(); });
    }

    @Test
    void testCopyResources() {
        ProcessingContext result = ProcessingContextUtils.copyResources(parentContext, childContext);
        assertEquals(childContext, result);
        verify(childContext).putResource(any(), eq("testValue"));
    }

    @Test
    void testExecuteWithResultNoId() {
        Function<ProcessingContext, CompletableFuture<String>> action = ctx -> CompletableFuture.completedFuture("done");

        CompletableFuture<String> resultFuture = ProcessingContextUtils.executeWithResult(
                null, unitOfWorkFactory, executor, parentContext, action
        );

        assertEquals("done", resultFuture.join());
        verify(unitOfWorkFactory).create(any(Function.class));
        verify(childContext).putResource(any(), eq("testValue"));
        // verify lifecycle hooks registration
        verify(childContext).whenComplete(any());
        verify(childContext).onAfterCommit(any());
        verify(childContext).onPrepareCommit(any());
    }

    @Test
    void testExecuteWithResultWithId() {
        Function<ProcessingContext, CompletableFuture<String>> action = ctx -> CompletableFuture.completedFuture("done");

        CompletableFuture<String> resultFuture = ProcessingContextUtils.executeWithResult(
                "myId", unitOfWorkFactory, executor, parentContext, action
        );

        assertEquals("done", resultFuture.join());
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

        Function<ProcessingContext, CompletableFuture<String>> action = ctx -> CompletableFuture.completedFuture("done");

        ProcessingContextUtils.executeWithResultInSeparateThread(
                "myId", unitOfWorkFactory, executorService, parentContext, action
        );

        verify(executorService).execute(any(Runnable.class));
        verify(unitOfWorkFactory).create(anyString(), any(Function.class));
    }
}
