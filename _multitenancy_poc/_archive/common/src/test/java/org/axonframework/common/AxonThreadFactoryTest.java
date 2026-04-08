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

package org.axonframework.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Allard Buijze
 */
class AxonThreadFactoryTest {

    private AxonThreadFactory testSubject;

    @Test
    void createWithThreadGroupByName() {
        testSubject = new AxonThreadFactory("test");
        Thread t1 = testSubject.newThread(new NoOpRunnable());
        Thread t2 = testSubject.newThread(new NoOpRunnable());

        assertEquals("test", t1.getThreadGroup().getName());
        assertEquals("test-0", t1.getName());
        assertEquals("test-1", t2.getName());
        assertSame(t1.getThreadGroup(), t2.getThreadGroup(), "Expected only a single ThreadGroup");
    }

    @Test
    void createWithThreadGroupByThreadGroupInstance() {
        ThreadGroup threadGroup = new ThreadGroup("test");
        testSubject = new AxonThreadFactory(threadGroup);
        Thread t1 = testSubject.newThread(new NoOpRunnable());
        Thread t2 = testSubject.newThread(new NoOpRunnable());

        assertEquals("test", t1.getThreadGroup().getName());
        assertEquals("test-0", t1.getName());
        assertSame(threadGroup, t1.getThreadGroup(), "Expected only a single ThreadGroup");
        assertSame(threadGroup, t2.getThreadGroup(), "Expected only a single ThreadGroup");
    }

    @Test
    void createWithPriority() {
        ThreadGroup threadGroup = new ThreadGroup("test");
        testSubject = new AxonThreadFactory(Thread.MAX_PRIORITY, threadGroup);
        Thread t1 = testSubject.newThread(new NoOpRunnable());
        Thread t2 = testSubject.newThread(new NoOpRunnable());

        assertEquals("test", t1.getThreadGroup().getName());
        assertEquals(Thread.MAX_PRIORITY, t1.getPriority());
        assertSame(threadGroup, t1.getThreadGroup(), "Expected only a single ThreadGroup");
        assertSame(threadGroup, t2.getThreadGroup(), "Expected only a single ThreadGroup");
    }

    @Test
    void rejectsTooHighPriority() {
        assertThrows(IllegalArgumentException.class,
                () -> new AxonThreadFactory(Thread.MAX_PRIORITY + 1, new ThreadGroup("")));
    }

    @Test
    void rejectsTooLowPriority() {
        assertThrows(IllegalArgumentException.class,
                () -> new AxonThreadFactory(Thread.MIN_PRIORITY - 1, new ThreadGroup("")));
    }

    private static class NoOpRunnable implements Runnable {

        @Override
        public void run() {
        }
    }
}
