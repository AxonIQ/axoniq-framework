# AF4 Distributed-Tracing Source (verbatim)

Source repo: `/Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4`

All files below are copied verbatim. Each `##` heading is the file's absolute path.

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/Span.java

```java
/*
 * Copyright (c) 2010-2023. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Represents a part of the application logic that will be traced. One or multiple spans together form a trace and are
 * often used to debug and monitor (distributed) applications.
 * <p>
 * The {@link Span} is an abstraction for Axon Framework to have tracing capabilities without knowing the specific
 * tracing provider. Calling {@link #start()} will start the {@code span} and make it active to the current thread. For
 * every start invocation, a respective {@link #end()} should be called as well to prevent scope leaks.
 * <p>
 * Creating {@link Span spans} is the responsibility of the {@link SpanFactory} which should be implemented by the
 * tracing provider of choice.
 * <p>
 * Important! In order to make this span the parent for any new span created during its execution,
 * {@link #makeCurrent()} should be called. This method will return a {@link SpanScope}, on which
 * {@link SpanScope#close()} should be invoked during the same code execution on the same thread. If not, this span will
 * become the unwanted parent of any children. You can make the same span the current for multiple threads at any point
 * in time, as long as you close them before calling {@link Span#end()}
 * <p>
 * Each {@link #start()} should eventually result in an {@link #end()} being called, but this does not have to be done
 * on the same thread.
 *
 * @author Mitchell Herrijgers
 * @see SpanFactory For more information about creating different kinds of traces.
 * @since 4.6.0
 */
public interface Span {

    /**
     * Starts the Span. However, does not set this span as the span of the current thread. See {@link #makeCurrent()} in
     * order to do so.
     *
     * @return The span for fluent interfacing.
     */
    Span start();

    /**
     * Sets the Span as the current for the current thread. The returned {@link SpanScope} must be closed before ending
     * the Span, on the same thread, or through a try-with-resources statement in the same thread as this method was
     * called.
     * <p>
     * You can make a span current on as many threads as you like, but you have to close every {@link SpanScope}, or
     * context will leak into the current thread. Note that if this is neglected, the {@link #end()} method should warn
     * the user in order to report this back to the framework.
     *
     * @return The scope of the span that must be closed be
     */
    default SpanScope makeCurrent() {
        return () -> {
        };
    }

    /**
     * Ends the span. All scopes should have been closed at this point. In addition, a span can only be ended once.
     * <p>
     * If scopes are still open when this method is called, either an exception should be thrown or an error log should
     * be produced to warn the user of the leak. This information can then be reported back to the developers of the
     * framework for a fix.
     */
    void end();

    /**
     * Records an exception to the span. This will be reported to the APM tooling, which can show more information about
     * the error in the trace. This method does not end the span.
     *
     * @param t The exception to record
     * @return The span for fluent interfacing.
     */
    Span recordException(Throwable t);

    /**
     * Runs a piece of code which will be traced. Exceptions will be caught automatically and added to the span, then
     * rethrown. The span will be started before the execution, and ended after execution. Note that the
     * {@link Runnable} will be invoked instantly and synchronously.
     *
     * @param runnable The {@link Runnable} to execute.
     */
    default void run(Runnable runnable) {
        this.start();
        try (SpanScope unused = this.makeCurrent()) {
            runnable.run();
        } catch (Exception e) {
            this.recordException(e);
            throw e;
        } finally {
            this.end();
        }
    }

    /**
     * Wraps a {@link Runnable}, propagating the current span context to the actual thread that runs the
     * {@link Runnable}. If you don't wrap a runnable before passing it to an {@link java.util.concurrent.Executor} the
     * context will be lost and a new trace will be started.
     *
     * @param runnable The {@link Runnable} to wrap
     * @return A wrapped runnable which propagates the span's context across threads.
     */
    default Runnable wrapRunnable(Runnable runnable) {
        return () -> run(runnable);
    }

    /**
     * Runs a piece of code which will be traced. Exceptions will be caught automatically and added to the span, then
     * rethrown. The span will be started before the execution, and ended after execution. Note that the
     * {@link Callable} will be invoked instantly and synchronously.
     *
     * @param callable The {@link Callable} to execute.
     */
    default <T> T runCallable(Callable<T> callable) throws Exception {
        this.start();
        try (SpanScope unused = this.makeCurrent()) {
            return callable.call();
        } catch (Exception e) {
            this.recordException(e);
            throw e;
        } finally {
            this.end();
        }
    }

    /**
     * Wraps a {@link Callable}, propagating the current span context to the actual thread that runs the
     * {@link Callable}. If you don't wrap a callable before passing it to an {@link java.util.concurrent.Executor} the
     * context will be lost and a new trace will be started.
     *
     * @param callable The {@link Callable} to wrap
     * @return A wrapped callable which propagates the span's context across threads.
     */
    default <T> Callable<T> wrapCallable(Callable<T> callable) {
        return () -> runCallable(callable);
    }

    /**
     * Runs a piece of code that returns a value and which will be traced. Exceptions will be caught automatically and
     * added to the span, then rethrown. The span will be started before the execution, and ended after execution. Note
     * that the {@link Supplier} will be invoked instantly and synchronously.
     *
     * @param supplier The {@link Supplier} to execute.
     */
    default <T> T runSupplier(Supplier<T> supplier) {
        this.start();
        try (SpanScope unused = this.makeCurrent()) {
            return supplier.get();
        } catch (Exception e) {
            this.recordException(e);
            throw e;
        } finally {
            this.end();
        }
    }

    /**
     * Wraps a {@link Supplier}, tracing the invocation. Exceptions will be caught automatically and added to the span,
     * then rethrown. The span will be started before the execution, and ended after execution.
     *
     * @param supplier The {@link Supplier} to wrap
     * @return A wrapped Supplier
     */
    default <T> Supplier<T> wrapSupplier(Supplier<T> supplier) {
        return () -> runSupplier(supplier);
    }

    /**
     * Runs a piece of code that returns a value and which will be traced. Exceptions will be caught automatically and
     * added to the span, then rethrown. The span will be started before the execution, and ended after execution. Note
     * that the {@link Consumer} will be invoked instantly and synchronously.
     *
     * @param supplier The {@link Consumer} to execute.
     */
    default <T> void runConsumer(Consumer<T> supplier, T consumedObject) {
        this.start();
        try (SpanScope unused = this.makeCurrent()) {
            supplier.accept(consumedObject);
        } catch (Exception e) {
            this.recordException(e);
            throw e;
        } finally {
            this.end();
        }
    }

    /**
     * Wraps a {@link Consumer}, tracing the invocation. Exceptions will be caught automatically and added to the span,
     * then rethrown. The span will be started before the execution, and ended after execution.
     *
     * @param supplier The {@link Consumer} to wrap
     * @return A wrapped Consumer
     */
    default <T> Consumer<T> wrapConsumer(Consumer<T> supplier) {
        return (consumedObject) -> runConsumer(supplier, consumedObject);
    }

    /**
     * Adds an attribute to the span. This can be used to add extra information to the span, which can be used by the
     * APM tooling to provide more information about the span.
     *
     * @param key   The key of the attribute.
     * @param value The value of the attribute.
     * @return The span for fluent interfacing.
     */
    default Span addAttribute(String key, String value) {
        return this;
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/SpanScope.java

```java
/*
 * Copyright (c) 2010-2023. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

/**
 * Represents the scope of a {@link Span}. This is attached to the thread, and should be closed
 * on the same thread as it was created before the span is ended.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.5
 */
@FunctionalInterface
public interface SpanScope extends AutoCloseable {
    /**
     * Closes the scope of the Span on which it was opened.
     * <p/>
     * {@inheritDoc}
     */
    @Override
    void close();
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/SpanFactory.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

import org.axonframework.messaging.Message;

import java.util.function.Supplier;

/**
 * The {@link SpanFactory} is responsible for making {@link Span spans} in a way the chosen tracing provider is
 * compatible with.
 * <p>
 * Each span has an operation name and span kind. From the operation name it should be clear what is happening in the
 * application. For example, use {@code "ClassName.method MessageName"} to indicate a message payload being handled.
 *
 * <p>
 * Spans can have tags, which are provided by {@link SpanAttributesProvider SpanAttributesProviders}. By default, any
 * time a message is used while creating a span should invoke all configured
 * {@link SpanAttributesProvider SpanAttributesProviders} and set those tags on the created span.
 *
 * <p>
 * The factory should support these types of spans:
 * <ul>
 *     <li>New root trace spans: These create an entirely new trace without parent. Use this for asynchronous calls that we want to measure the performance individually of. For example, snapshotting (which has no influence on a business process).</li>
 *     <li>New handler spans: This creates a new span in an already existing trace. The span that was active when the message was dispatched will be linked to it. It will be of the handling type</li>
 *     <li>New dispatch spans: This creates a new span in an already existing trace. It will be of a dispatching type. </li>
 *     <li>New internal span: This creates a new sub-span in an already active span. Use this for measuring individual parts of an already existing span. For example, measuring how long it takes to load the aggregate when handling an event.</li>
 * </ul>
 * <p>
 * Check the individual method's javadoc more information on each type.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public interface SpanFactory {

    /**
     * Creates a new {@link Span} without any parent trace. This should be used for logical start point of asynchronous
     * calls that are not related to a message. For example snapshotting an aggregate.
     * <p>
     * In monitoring systems, this Span will be the root of the trace.
     *
     * @param operationNameSupplier Supplier of the operation's name.
     * @return The created {@link Span}.
     */
    Span createRootTrace(Supplier<String> operationNameSupplier);

    /**
     * Creates a new {@link Span} which becomes its own separate trace, linked to the previous span. Useful for
     * asynchronous invocations, such as handling an event in a StreamingEventProcessor.
     * <p>
     * In monitoring systems, this Span will start a separate trace linked to the previous one.
     * <p>
     * The message's name will be concatenated with the {@code operationName}, see
     * {@link SpanUtils#determineMessageName(Message)}.
     *
     * @param operationNameSupplier Supplier of the operation's name.
     * @param parentMessage The message that is being handled.
     * @param linkedParents Optional parameter, providing this will link the provided message(s) to the current, in
     *                      addition to the original.
     * @return The created {@link Span}.
     */
    default Span createLinkedHandlerSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage, Message<?>... linkedParents) {
        return createHandlerSpan(operationNameSupplier, parentMessage, false, linkedParents);
    }

    /**
     * Creates a new {@link Span} which is part of the current trace. The message attributes will be added to the span,
     * based on the provided {@link SpanAttributesProvider SpanAttributesProviders} for additional debug information.
     * <p>
     * In monitoring systems, this Span will be part of another trace.
     * <p>
     * The message's name will be concatenated with the {@code operationName}, see
     * {@link SpanUtils#determineMessageName(Message)}.
     *
     * @param operationNameSupplier Supplier of the operation's name.
     * @param parentMessage The message that is being handled.
     * @param linkedParents Optional parameter, providing this will link the provided message(s) to the current, in
     *                      addition to the original.
     * @return The created {@link Span}.
     */
    default Span createChildHandlerSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage, Message<?>... linkedParents) {
        return createHandlerSpan(operationNameSupplier, parentMessage, true, linkedParents);
    }

    /**
     * Creates a new {@link Span} linked to asynchronously handling a {@link Message}, for example when handling a
     * command from Axon Server. The message attributes will be added to the span, based on the provided
     * {@link SpanAttributesProvider SpanAttributesProviders} for additional debug information.
     * <p>
     * In monitoring systems, this Span will be the root of the trace.
     * <p>
     * The message's name will be concatenated with the {@code operationName}, see
     * {@link SpanUtils#determineMessageName(Message)}.
     *
     * @param operationNameSupplier Supplier of the operation's name.
     * @param parentMessage The message that is being handled.
     * @param isChildTrace  Whether to force the span to be a part of the current trace. This means not linking, but
     *                      setting a parent.
     * @param linkedParents Optional parameter, providing this will link the provided message(s) to the current, in
     *                      addition to the original.
     * @return The created {@link Span}.
     */
    Span createHandlerSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage, boolean isChildTrace,
                           Message<?>... linkedParents);

    /**
     * Creates a new {@link Span} linked to dispatching a {@link Message}, for example when sending a command to Axon
     * Server. The message attributes will be added to the span, based on the provided
     * {@link SpanAttributesProvider SpanAttributesProviders} for additional debug information.
     * <p>
     * In monitoring systems, this Span will be part of another trace.
     * <p>
     * Before asynchronously dispatching a message, add the tracing context to the message, using
     * {@link #propagateContext(Message)} to the message's metadata.
     * <p>
     * The message's name will be concatenated with the {@code operationName}, see
     * {@link SpanUtils#determineMessageName(Message)}.
     *
     * @param operationNameSupplier Supplier of the operation's name.
     * @param parentMessage  The message that is being handled.
     * @param linkedSiblings Optional parameter, providing this will link the provided messages to the current.
     * @return The created {@link Span}.
     */
    Span createDispatchSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage, Message<?>... linkedSiblings);

    /**
     * Creates a new {@link Span} linked to the currently active span. This is useful for tracing different parts of
     * framework logic, so we can time what has the most impact.
     * <p>
     * In monitoring systems, this Span will be part of another trace.
     *
     * @param operationNameSupplier Supplier of the operation's name.
     * @return The created {@link Span}.
     */
    Span createInternalSpan(Supplier<String> operationNameSupplier);

    /**
     * Creates a new {@link Span} linked to the currently active span. This is useful for tracing different parts of
     * framework logic, so we can time what has the most impact.
     * <p>
     * The message supplied is used to provide a clearer name, based on {@link SpanUtils#determineMessageName(Message)},
     * and to add the message's attributes to the span.
     * <p>
     * In monitoring systems, this Span will be part of another trace.
     *
     * @param operationNameSupplier Supplier of the operation's name.
     * @return The created {@link Span}.
     */
    Span createInternalSpan(Supplier<String> operationNameSupplier, Message<?> message);


    /**
     * Registers an additional {@link SpanAttributesProvider} to the factory.
     *
     * @param provider The provider to add.
     */
    void registerSpanAttributeProvider(SpanAttributesProvider provider);

    /**
     * Propagates the currently active trace and span to the message. It should do so in a way that the context can be
     * retrieved by the {@link #createLinkedHandlerSpan(Supplier, Message, Message[])} method. The most efficient method
     * currently known is to enhance the message's metadata.
     * <p>
     * Since messages are immutable, the method returns the enhanced message. This enhanced message should be used
     * during dispatch instead of the original message.
     *
     * @param message The message to enhance.
     * @param <M>     The message's type.
     * @return The enhanced message.
     */
    <M extends Message<?>> M propagateContext(M message);
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/NoOpSpanFactory.java

```java
/*
 * Copyright (c) 2010-2023. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

import org.axonframework.messaging.Message;

import java.util.function.Supplier;

/**
 * {@link SpanFactory} implementation that creates a {@link NoOpSpan}. This span does not do any tracing at all. It's
 * used as a fallback when there is no tracing implementation available, so framework code does not have to check.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class NoOpSpanFactory implements SpanFactory {

    /**
     * Singleton instance of the {@link NoOpSpanFactory}, which is used for configuration when there is no specific
     * implementation configured.
     */
    public static final NoOpSpanFactory INSTANCE = new NoOpSpanFactory();

    @Override
    public Span createRootTrace(Supplier<String> operationNameSupplier) {
        return NoOpSpan.INSTANCE;
    }

    @Override
    public Span createHandlerSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage,
                                  boolean isChildTrace,
                                  Message<?>... linkedParents) {
        return NoOpSpan.INSTANCE;
    }

    @Override
    public Span createDispatchSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage,
                                   Message<?>... linkedSiblings) {
        return NoOpSpan.INSTANCE;
    }

    @Override
    public Span createInternalSpan(Supplier<String> operationNameSupplier) {
        return NoOpSpan.INSTANCE;
    }

    @Override
    public Span createInternalSpan(Supplier<String> operationNameSupplier, Message<?> message) {
        return NoOpSpan.INSTANCE;
    }

    @Override
    public void registerSpanAttributeProvider(SpanAttributesProvider supplier) {
        // Do nothing
    }

    @Override
    public <M extends Message<?>> M propagateContext(M message) {
        return message;
    }

    /**
     * The {@link Span} implementation that does nothing.
     */
    public static class NoOpSpan implements Span {

        /**
         * Instance of a {@link NoOpSpan} that can be used to avoid creating new instances.
         */
        public static final NoOpSpan INSTANCE = new NoOpSpan();

        @Override
        public Span start() {
            return this;
        }

        @Override
        public SpanScope makeCurrent() {
            return () -> {};
        }

        @Override
        public void end() {
            // Do nothing
        }

        @Override
        public Span recordException(Throwable t) {
            return this;
        }

        @Override
        public Span addAttribute(String key, String value) {
            return this;
        }
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/MultiSpanFactory.java

```java
/*
 * Copyright (c) 2010-2023. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

import org.axonframework.messaging.Message;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Implementation of a {@link SpanFactory} that delegates calls to multiple other factories. Because only a single
 * {@code SpanFactory} can be configured, this is useful when you need to combine several {@code SpanFactory}
 * implementations to be used as a single factory.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class MultiSpanFactory implements SpanFactory {

    private final List<SpanFactory> spanFactories;

    /**
     * Creates the {@link MultiSpanFactory} with the delegate factory implementations that it should use.
     *
     * @param spanFactories The delegate {@link SpanFactory} implementations it should use.
     */
    public MultiSpanFactory(List<SpanFactory> spanFactories) {
        this.spanFactories = spanFactories;
    }

    @Override
    public Span createRootTrace(Supplier<String> operationNameSupplier) {
        return new MultiSpan(
                spanFactories.stream()
                             .map(sf -> sf.createRootTrace(operationNameSupplier))
                             .collect(Collectors.toList())
        );
    }

    @Override
    public Span createHandlerSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage,
                                  boolean isChildTrace, Message<?>... linkedParents) {
        return new MultiSpan(
                spanFactories.stream()
                             .map(sf -> sf.createHandlerSpan(operationNameSupplier,
                                                             parentMessage,
                                                             isChildTrace,
                                                             linkedParents))
                             .collect(Collectors.toList())
        );
    }

    @Override
    public Span createDispatchSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage,
                                   Message<?>... linkedSiblings) {
        return new MultiSpan(
                spanFactories.stream()
                             .map(sf -> sf.createDispatchSpan(operationNameSupplier,
                                                              parentMessage,
                                                              linkedSiblings))
                             .collect(Collectors.toList())
        );
    }

    @Override
    public Span createInternalSpan(Supplier<String> operationNameSupplier) {
        return new MultiSpan(
                spanFactories.stream()
                             .map(sf -> sf.createInternalSpan(operationNameSupplier))
                             .collect(Collectors.toList())
        );
    }

    @Override
    public Span createInternalSpan(Supplier<String> operationNameSupplier, Message<?> message) {
        return new MultiSpan(
                spanFactories.stream()
                             .map(sf -> sf.createInternalSpan(operationNameSupplier, message))
                             .collect(Collectors.toList())
        );
    }

    @Override
    public void registerSpanAttributeProvider(SpanAttributesProvider provider) {
        spanFactories.forEach(sf -> sf.registerSpanAttributeProvider(provider));
    }

    @Override
    public <M extends Message<?>> M propagateContext(M message) {
        M adjustedMessage = message;
        for (SpanFactory spanFactory : spanFactories) {
            adjustedMessage = spanFactory.propagateContext(adjustedMessage);
        }

        return adjustedMessage;
    }

    private static class MultiSpan implements Span {

        private final List<Span> spans;

        public MultiSpan(List<Span> spans) {
            this.spans = spans;
        }

        @Override
        public Span start() {
            spans.forEach(Span::start);
            return this;
        }

        @Override
        public SpanScope makeCurrent() {
            List<SpanScope> scopes = spans.stream().map(Span::makeCurrent).collect(Collectors.toList());
            return () -> scopes.forEach(SpanScope::close);
        }

        @Override
        public void end() {
            spans.forEach(Span::end);
        }

        @Override
        public Span recordException(Throwable t) {
            spans.forEach(s -> s.recordException(t));
            return this;
        }

        @Override
        public Span addAttribute(String key, String value) {
            spans.forEach(s -> s.addAttribute(key, value));
            return this;
        }
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/LoggingSpanFactory.java

```java
/*
 * Copyright (c) 2010-2023. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

import org.axonframework.common.IdentifierFactory;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/**
 * Implementation of a {@link SpanFactory} that requires no java agent or APM system, only logging. Will log tracing
 * information to info level, with the following identifying prefix: {@code {spanId}{spanName}}.
 * <p>
 * When traces are related to a message, the message type and identifier are logged as well. If a message is dispatched
 * while currently handling a message in the {@link org.axonframework.messaging.unitofwork.UnitOfWork}, it will log the
 * information regarding the message being handled as well.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class LoggingSpanFactory implements SpanFactory {

    /**
     * Singleton instance of the {@link LoggingSpanFactory}.
     */
    public static final LoggingSpanFactory INSTANCE = new LoggingSpanFactory();

    private static final Logger logger = LoggerFactory.getLogger(LoggingSpanFactory.class);

    private LoggingSpanFactory() {
    }

    @Override
    public Span createRootTrace(Supplier<String> operationNameSupplier) {
        return new Slf4jSpan(operationNameSupplier, () -> "Root trace started");
    }

    @Override
    public Span createHandlerSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage,
                                  boolean isChildTrace, Message<?>... linkedParents) {
        return new Slf4jSpan(operationNameSupplier,
                             () -> String.format("Handler span started for message of type [%s] and identifier [%s]",
                                                 parentMessage.getClass().getSimpleName(),
                                                 parentMessage.getIdentifier()));
    }

    @Override
    public Span createDispatchSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage,
                                   Message<?>... linkedSiblings) {
        return new Slf4jSpan(operationNameSupplier, () -> getSpanMessage("Dispatch", parentMessage));
    }

    private String getSpanMessage(String spanType, Message<?> parentMessage) {
        return CurrentUnitOfWork
                .map(uow -> String.format(
                        "%s span started for message of type [%s] and identifier [%s] while handling message of type [%s] and identifier [%s]",
                        spanType,
                        parentMessage.getClass().getSimpleName(),
                        parentMessage.getIdentifier(),
                        uow.getMessage().getClass().getSimpleName(),
                        uow.getMessage().getIdentifier()))
                .orElseGet(() -> String.format(
                        "%s span started for message of type [%s] and identifier [%s]",
                        spanType,
                        parentMessage.getClass().getSimpleName(),
                        parentMessage.getIdentifier()));
    }

    @Override
    public Span createInternalSpan(Supplier<String> operationNameSupplier) {
        return new Slf4jSpan(operationNameSupplier,
                             () -> CurrentUnitOfWork
                                     .map(uow -> String.format(
                                             "Internal span started while handling message of type [%s] and identifier [%s]",
                                             uow.getMessage().getClass().getSimpleName(),
                                             uow.getMessage().getIdentifier()))
                                     .orElseGet(() -> "Internal span started"));
    }

    @Override
    public Span createInternalSpan(Supplier<String> operationNameSupplier, Message<?> message) {
        return new Slf4jSpan(operationNameSupplier, () -> getSpanMessage("Internal", message));
    }

    @Override
    public void registerSpanAttributeProvider(SpanAttributesProvider provider) {
        // No-op for now
    }

    @Override
    public <M extends Message<?>> M propagateContext(M message) {
        return message;
    }

    private static class Slf4jSpan implements Span {

        protected final String identifier;
        protected final String name;
        protected final Supplier<String> startLog;
        protected final List<Slf4jSpanScope> scopesList;

        private Slf4jSpan(Supplier<String> nameSupplier, Supplier<String> startLog) {
            this.startLog = startLog;
            this.identifier = IdentifierFactory.getInstance().generateIdentifier();
            this.name = nameSupplier.get();
            this.scopesList = new CopyOnWriteArrayList<>();
        }

        @Override
        public Span start() {
            logger.info("[{}][{}] {}", identifier, name, startLog.get());
            return this;
        }

        @Override
        public SpanScope makeCurrent() {
            logger.debug("[{}][{}] Made current for thread [{}]", identifier, name, Thread.currentThread().getName());
            Slf4jSpanScope scope = new Slf4jSpanScope();
            scopesList.add(scope);
            return scope;
        }

        @Override
        public void end() {
            if(!scopesList.isEmpty()) {
                logger.error("[{}][{}] Span ended without closing {} scopes!", identifier, name, scopesList.size());
            }
            logger.info("[{}][{}] Span ended", identifier, name);
        }

        @Override
        public Span recordException(Throwable t) {
            logger.info("[{}][{}] Span recorded exception", identifier, name, t);
            return this;
        }

        @Override
        public Span addAttribute(String key, String value) {
            return this;
        }

        private class Slf4jSpanScope implements SpanScope {
            @Override
            public void close() {
                logger.debug("[{}][{}] Closed for thread [{}]", identifier, name, Thread.currentThread().getName());
                scopesList.remove(this);
            }
        }
    }

}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/SpanAttributesProvider.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

import org.axonframework.messaging.Message;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Represents a provider of attributes to a {@link Span}, based on a {@link Message}. It's the responsibility of the
 * {@link SpanFactory} to invoke these and add the attributes to the {@link Span}.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public interface SpanAttributesProvider {

    /**
     * Provides a map of attributes to add to the {@link Span} based on the {@link Message} provided.
     *
     * @param message The message
     * @return The attributes
     */
    @Nonnull
    Map<String, String> provideForMessage(@Nonnull Message<?> message);
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/SpanUtils.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.messaging.Message;
import org.axonframework.queryhandling.QueryMessage;

import java.util.Objects;

/**
 * Utilities for creating spans which are relevant for all implementations of tracing.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class SpanUtils {

    private SpanUtils() {
        // Utility class
    }

    /**
     * Creates a human-readable name for a message's payload. This is the simple name of the payload by default, unless
     * the more specific name of a message differs from it. This can be the case for commands and queries.
     *
     * @param message The message to determine a message name for
     * @return The message's name
     */
    public static String determineMessageName(Message<?> message) {
        String messageName = message.getPayloadType().getSimpleName();
        if (message instanceof CommandMessage) {
            String commandName = ((CommandMessage<?>) message).getCommandName();
            if (!Objects.equals(commandName, message.getPayloadType().getName())) {
                messageName = commandName;
            }
        }
        if (message instanceof QueryMessage) {
            String queryName = ((QueryMessage<?, ?>) message).getQueryName();
            if (!Objects.equals(queryName, message.getPayloadType().getName())) {
                messageName = queryName;
            }
        }
        if (message instanceof DeadlineMessage) {
            if (message.getPayload() != null) {
                return ((DeadlineMessage<?>) message).getDeadlineName() + "," + message.getPayloadType()
                                                                                       .getSimpleName();
            }
            return ((DeadlineMessage<?>) message).getDeadlineName();
        }
        return messageName;
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/HandlerSpanFactory.java

```java
/*
 * Copyright (c) 2010-2023. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

/**
 * Factory that creates spans for the spans representing handlers.
 *
 * @author Mitchell Herrijgers
 * @since 4.9.0
 */
public interface HandlerSpanFactory {

}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/TracingHandlerEnhancerDefinition.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

import org.axonframework.common.BuilderUtils;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.annotation.HandlerEnhancerDefinition;
import org.axonframework.messaging.annotation.MessageHandlingMember;
import org.axonframework.messaging.annotation.WrappedMessageHandlingMember;

import java.lang.reflect.Executable;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Enhances message handlers with the provided {@link SpanFactory}, wrapping handling of the message in a {@link Span}
 * that is reported to the monitoring tooling.
 * <p>
 * Since {@code EventSourcingHandlers} can be very noisy when loading the aggregate, they can be enabled or disabled
 * separately through constructor configuration.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class TracingHandlerEnhancerDefinition implements HandlerEnhancerDefinition {

    private final SpanFactory spanFactory;
    private final boolean showEventSourcingHandlers;

    /**
     * Creates a new {@link TracingHandlerEnhancerDefinition} based on the builder.
     *
     * @param builder The builder to construct the {@link TracingHandlerEnhancerDefinition} from.
     */
    protected TracingHandlerEnhancerDefinition(Builder builder) {
        BuilderUtils.assertNonNull(builder.spanFactory, "SpanFactory must be provided!");
        this.spanFactory = builder.spanFactory;
        this.showEventSourcingHandlers = builder.showEventSourcingHandlers;
    }

    /**
     * Instantiate a builder to create a {@link TracingHandlerEnhancerDefinition}.
     * <p>
     * The {@code showEventSourcingHandlers} is defaulted to {@code false}. The {@link SpanFactory} is a hard
     * requirement and should be provided.
     *
     * @return The builder to create a {@link TracingHandlerEnhancerDefinition}.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public <T> MessageHandlingMember<T> wrapHandler(@Nonnull MessageHandlingMember<T> original) {
        if (!showEventSourcingHandlers && isEventSourcingHandler(original)) {
            return original;
        }

        Optional<Executable> unwrap = original.unwrap(Executable.class);
        if (!unwrap.isPresent()) {
            return original;
        }
        String signature = toMethodSignature(unwrap.get());
        return new WrappedMessageHandlingMember<T>(original) {
            @Override
            public Object handle(@Nonnull Message<?> message, T target) throws Exception {
                return spanFactory.createInternalSpan(() -> getSpanName(target, signature))
                                  .runCallable(() -> super.handle(message, target));
            }
        };
    }

    private static <T> String getSpanName(T target, String signature) {
        return target == null ? signature : target.getClass().getSimpleName() + "." + signature;
    }

    private boolean isEventSourcingHandler(MessageHandlingMember<?> original) {
        return original.attribute("EventSourcingHandler.payloadType").isPresent();
    }

    private String toMethodSignature(Executable executable) {
        return String.format("%s(%s)",
                             executable.getName(),
                             Arrays.stream(executable.getParameterTypes()).map(Class::getSimpleName)
                                   .collect(Collectors.joining(",")));
    }

    /**
     * Builder class to instantiate a {@link TracingHandlerEnhancerDefinition}.
     * <p>
     * The {@code showEventSourcingHandlers} is defaulted to {@code false}. The {@link SpanFactory} is a hard
     * requirement and should be provided.
     */
    public static class Builder {

        private SpanFactory spanFactory;
        private boolean showEventSourcingHandlers = false;


        /**
         * Configures the {@link SpanFactory} the handler enhancer should use for tracing.
         *
         * @param spanFactory The {@link SpanFactory} to configure.
         * @return The builder, for fluent interfacing.
         */
        public Builder spanFactory(SpanFactory spanFactory) {
            BuilderUtils.assertNonNull(spanFactory, "SpanFactory can not be set to null!");
            this.spanFactory = spanFactory;
            return this;
        }

        /**
         * Configures whether event sourcing handlers should be traced. Defaults to {@code false}.
         *
         * @param showEventSourcingHandlers Whether event sourcing handlers should be traced.
         * @return The builder, for fluent interfacing.
         */
        public Builder showEventSourcingHandlers(boolean showEventSourcingHandlers) {
            this.showEventSourcingHandlers = showEventSourcingHandlers;
            return this;
        }

        /**
         * Initializes the {@link TracingHandlerEnhancerDefinition} based on the builder contents.
         *
         * @return The {@link TracingHandlerEnhancerDefinition}.
         */
        public TracingHandlerEnhancerDefinition build() {
            return new TracingHandlerEnhancerDefinition(this);
        }
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/attributes/MessageIdSpanAttributesProvider.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.attributes;

import org.axonframework.messaging.Message;
import org.axonframework.tracing.SpanAttributesProvider;

import java.util.Map;
import javax.annotation.Nonnull;

import static java.util.Collections.singletonMap;

/**
 * Adds the message identifier to the Span.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class MessageIdSpanAttributesProvider implements SpanAttributesProvider {

    @Override
    public @Nonnull Map<String, String> provideForMessage(@Nonnull Message<?> message) {
        return singletonMap("axon_message_id", message.getIdentifier());
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/attributes/MessageNameSpanAttributesProvider.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.attributes;

import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.messaging.Message;
import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.tracing.Span;
import org.axonframework.tracing.SpanAttributesProvider;

import java.util.Map;
import javax.annotation.Nonnull;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

/**
 * Adds the name of a {@link Message} to the {@link Span}. Note this only takes effect for
 * {@link CommandMessage CommandMessages} and {@link QueryMessage QueryMessages}.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class MessageNameSpanAttributesProvider implements SpanAttributesProvider {

    @Override
    public @Nonnull Map<String, String> provideForMessage(@Nonnull Message<?> message) {
        String name = determineName(message);
        if (name != null) {
            return singletonMap("axon_message_name", name);
        }
        return emptyMap();
    }

    private String determineName(Message<?> message) {
        if (message instanceof CommandMessage) {
            return ((CommandMessage<?>) message).getCommandName();
        }
        if (message instanceof QueryMessage) {
            return QueryMessage.queryName(message);
        }
        return null;
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/attributes/MessageTypeSpanAttributesProvider.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.attributes;

import org.axonframework.messaging.Message;
import org.axonframework.tracing.SpanAttributesProvider;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Adds the message type (simple class name) to the Span.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class MessageTypeSpanAttributesProvider implements SpanAttributesProvider {

    @Override
    public @Nonnull Map<String, String> provideForMessage(@Nonnull Message<?> message) {
        return Collections.singletonMap("axon_message_type", message.getClass().getSimpleName());
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/attributes/PayloadTypeSpanAttributesProvider.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.attributes;

import org.axonframework.messaging.Message;
import org.axonframework.tracing.Span;
import org.axonframework.tracing.SpanAttributesProvider;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Adds the {@link Message#getPayloadType payload type} as an attribute to the {@link Span}.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class PayloadTypeSpanAttributesProvider implements SpanAttributesProvider {

    @Override
    public @Nonnull Map<String, String> provideForMessage(@Nonnull Message<?> message) {
        return Collections.singletonMap("axon_payload_type", message.getPayloadType().getName());
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/attributes/MetadataSpanAttributesProvider.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.attributes;

import org.axonframework.messaging.Message;
import org.axonframework.tracing.SpanAttributesProvider;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Adds the metadata of the message to the span as attributes.
 * <p>
 * The values are not serialized to a specific format, rather the {@link Object#toString()} method is called on it.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class MetadataSpanAttributesProvider implements SpanAttributesProvider {

    @Override
    public @Nonnull Map<String, String> provideForMessage(@Nonnull Message<?> message) {
        Map<String, String> map = new HashMap<>();
        message.getMetaData().forEach((key, value) -> map.put("axon_metadata_" + key, value.toString()));
        return map;
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/tracing/attributes/AggregateIdentifierSpanAttributesProvider.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.attributes;

import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.messaging.Message;
import org.axonframework.tracing.SpanAttributesProvider;

import java.util.Map;
import javax.annotation.Nonnull;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

/**
 * Adds the aggregate identifier to the Span if the current message being handled is a {@link DomainEventMessage}.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class AggregateIdentifierSpanAttributesProvider implements SpanAttributesProvider {

    @Override
    public @Nonnull Map<String, String> provideForMessage(@Nonnull Message<?> message) {
        if (message instanceof DomainEventMessage) {
            DomainEventMessage<?> domainEventMessage = (DomainEventMessage<?>) message;
            return singletonMap("axon_aggregate_identifier", domainEventMessage.getAggregateIdentifier());
        }
        return emptyMap();
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/commandhandling/CommandBusSpanFactory.java

```java
/*
 * Copyright (c) 2010-2023. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.commandhandling;

import org.axonframework.tracing.Span;

/**
 * Span factory that creates spans for the {@link CommandBus}. You can customize the spans of the bus by creating your
 * own implementation.
 *
 * @author Mitchell Herrijgers
 * @since 4.9.0
 */
public interface CommandBusSpanFactory {

    /**
     * Creates a span for the dispatching of a command.
     *
     * @param commandMessage The command message to create a span for.
     * @param distributed    Whether the command is distributed or not.
     * @return The created span.
     */
    Span createDispatchCommandSpan(CommandMessage<?> commandMessage, boolean distributed);

    /**
     * Creates a span for the handling of a command.
     *
     * @param commandMessage The command message to create a span for.
     * @param distributed    Whether the command is distributed or not.
     * @return The created span.
     */
    Span createHandleCommandSpan(CommandMessage<?> commandMessage, boolean distributed);

    /**
     * Propagates the context of the current span to the given command message.
     *
     * @param commandMessage The command message to propagate the context to.
     * @param <T>            The type of the payload of the command message.
     * @return The command message with the propagated context.
     */
    <T> CommandMessage<T> propagateContext(CommandMessage<T> commandMessage);
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/main/java/org/axonframework/commandhandling/DefaultCommandBusSpanFactory.java

```java
/*
 * Copyright (c) 2010-2023. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.commandhandling;

import org.axonframework.common.BuilderUtils;
import org.axonframework.tracing.Span;
import org.axonframework.tracing.SpanFactory;

/**
 * Default implementation of the {@link CommandBusSpanFactory}. Can be configured to include the command handling of a
 * distributed command in the same trace or not (true by default).
 *
 * @author Mitchell Herrijgers
 * @since 4.9.0
 */
public class DefaultCommandBusSpanFactory implements CommandBusSpanFactory {

    private final SpanFactory spanFactory;
    private final boolean distributedInSameTrace;

    /**
     * Creates a new {@link DefaultCommandBusSpanFactory} using the provided {@code builder}.
     *
     * @param builder The builder to build the {@link DefaultCommandBusSpanFactory} from.
     */
    protected DefaultCommandBusSpanFactory(Builder builder) {
        builder.validate();
        this.spanFactory = builder.builderSpanFactory;
        this.distributedInSameTrace = builder.distributedInSameTrace;
    }

    @Override
    public Span createDispatchCommandSpan(CommandMessage<?> commandMessage, boolean distributed) {
        if (distributed) {
            return spanFactory.createDispatchSpan(() -> "CommandBus.dispatchDistributedCommand", commandMessage);
        }
        return spanFactory.createInternalSpan(() -> "CommandBus.dispatchCommand", commandMessage);
    }

    @Override
    public Span createHandleCommandSpan(CommandMessage<?> commandMessage, boolean distributed) {
        if (distributed) {
            if (distributedInSameTrace) {
                return spanFactory.createChildHandlerSpan(() -> "CommandBus.handleDistributedCommand", commandMessage);
            }
            return spanFactory.createLinkedHandlerSpan(() -> "CommandBus.handleDistributedCommand", commandMessage);
        }
        return spanFactory.createChildHandlerSpan(() -> "CommandBus.handleCommand", commandMessage);
    }

    @Override
    public <T> CommandMessage<T> propagateContext(CommandMessage<T> commandMessage) {
        return spanFactory.propagateContext(commandMessage);
    }

    /**
     * Creates a Builder to be able to create a {@link DefaultCommandBusSpanFactory}. The default values are:
     * <ul>
     *     <li>{@code distributedCommandInSameTrace} defaults to {@code true}</li>
     * </ul>
     * The {@code spanFactory} is a required field and should be provided.
     *
     * @return a Builder to be able to create a {@link DefaultCommandBusSpanFactory}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class to instantiate a {@link DefaultCommandBusSpanFactory}. The default values are:
     * <ul>
     *     <li>{@code distributedCommandInSameTrace} defaults to {@code true}</li>
     * </ul>
     * The {@code spanFactory} is a required field and should be provided.
     */
    public static class Builder {

        private SpanFactory builderSpanFactory;
        private boolean distributedInSameTrace = true;

        /**
         * Sets the {@link SpanFactory} to use to create the spans. This is a required field.
         *
         * @param spanFactory The {@link SpanFactory} to use to create the spans.
         * @return The current Builder instance, for fluent interfacing.
         */
        public Builder spanFactory(SpanFactory spanFactory) {
            BuilderUtils.assertNonNull(spanFactory, "spanFactory may not be null");
            this.builderSpanFactory = spanFactory;
            return this;
        }

        /**
         * Sets whether the {@link CommandMessage}s should be handled in the same trace as the dispatching span.
         *
         * @param distributedInSameTrace whether the {@link CommandMessage CommandsMessages} should be handled in the same trace as the
         *                               dispatching span.
         * @return The current Builder instance, for fluent interfacing.
         */
        public Builder distributedInSameTrace(boolean distributedInSameTrace) {
            this.distributedInSameTrace = distributedInSameTrace;
            return this;
        }

        /**
         * Validates whether the fields contained in this builder are set accordingly.
         */
        protected void validate() {
            BuilderUtils.assertNonNull(builderSpanFactory, "spanFactory may not be null");
        }

        /**
         * Initializes a {@link DefaultCommandBusSpanFactory} as specified through this Builder.
         *
         * @return The {@link DefaultCommandBusSpanFactory} as specified through this Builder.
         */
        public DefaultCommandBusSpanFactory build() {
            return new DefaultCommandBusSpanFactory(this);
        }
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/tracing-opentelemetry/src/main/java/org/axonframework/tracing/opentelemetry/OpenTelemetrySpanFactory.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.opentelemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.axonframework.common.BuilderUtils;
import org.axonframework.messaging.Message;
import org.axonframework.tracing.Span;
import org.axonframework.tracing.SpanAttributesProvider;
import org.axonframework.tracing.SpanFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import static org.axonframework.tracing.SpanUtils.determineMessageName;

/**
 * Creates {@link Span} implementations that are compatible with OpenTelemetry java agent instrumentation. OpenTelemetry
 * is a standard to collect logging, tracing and metrics from applications. This {@link SpanFactory} focuses on
 * supporting the tracing part of the standard.
 * <p>
 * To get started with OpenTelemetry, <a href="https://opentelemetry.io/docs/">check out their documentation</a>. Note
 * that, even after configuring the correct dependencies, you still need to run the application using the OpenTelemetry
 * java agent to export data. Without this, it will have the same effect as the
 * {@link org.axonframework.tracing.NoOpSpanFactory} since the data is not sent anywhere.
 *
 * <p>
 * When creating a trace, the context of the current trace is used as a parent, instead of the one active at the time of
 * starting the span (Default OpenTelemetry behavior). This is done using {@link SpanBuilder#setParent(Context)}.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class OpenTelemetrySpanFactory implements SpanFactory {

    private final Tracer tracer;
    private final TextMapPropagator textMapPropagator;
    private final List<SpanAttributesProvider> spanAttributesProviders;
    private final TextMapGetter<Message<?>> textMapGetter;
    private final TextMapSetter<Map<String, String>> textMapSetter;

    /**
     * Instantiate a {@link OpenTelemetrySpanFactory} based on the fields contained in the {@link Builder}.
     *
     * @param builder the {@link Builder} used to instantiate a {@link OpenTelemetrySpanFactory} instance.
     */
    public OpenTelemetrySpanFactory(Builder builder) {
        this.spanAttributesProviders = builder.spanAttributesProviders;
        this.tracer = builder.tracer;
        this.textMapPropagator = builder.textMapPropagator;
        this.textMapGetter = builder.textMapGetter;
        this.textMapSetter = builder.textMapSetter;
    }

    /**
     * Instantiate a Builder to create a {@link OpenTelemetrySpanFactory}.
     * <p>
     * The {@link SpanAttributesProvider SpanAttributeProvieders} are defaulted to an empty list, and the {@link Tracer}
     * is defaulted to the tracer defined by {@link GlobalOpenTelemetry}.
     *
     * @return a Builder able to create a {@link OpenTelemetrySpanFactory}.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message<?>> M propagateContext(M message) {
        HashMap<String, String> additionalMetadataProperties = new HashMap<>();
        textMapPropagator.inject(Context.current(), additionalMetadataProperties, textMapSetter);
        return (M) message.andMetaData(additionalMetadataProperties);
    }

    @Override
    public Span createRootTrace(Supplier<String> operationNameSupplier) {
        SpanBuilder spanBuilder = createSpanBuilder(operationNameSupplier.get(), SpanKind.INTERNAL)
                .addLink(io.opentelemetry.api.trace.Span.current().getSpanContext())
                .setNoParent();
        return new OpenTelemetrySpan(spanBuilder);
    }

    @Override
    public Span createHandlerSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage,
                                  boolean isChildTrace,
                                  Message<?>... linkedParents) {
        Context parentContext = textMapPropagator.extract(Context.current(),
                                                          parentMessage,
                                                          textMapGetter);
        SpanBuilder spanBuilder = createSpanBuilder(formatName(operationNameSupplier.get(), parentMessage),
                                                    SpanKind.CONSUMER);
        if (isChildTrace) {
            spanBuilder.setParent(parentContext);
        } else {
            spanBuilder.addLink(io.opentelemetry.api.trace.Span.fromContext(parentContext).getSpanContext())
                       .setNoParent();
        }
        addLinks(spanBuilder, linkedParents);
        addMessageAttributes(spanBuilder, parentMessage);
        return new OpenTelemetrySpan(spanBuilder);
    }

    @Override
    public Span createDispatchSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage,
                                   Message<?>... linkedSiblings) {
        SpanBuilder spanBuilder = createSpanBuilderWithCurrentContext(
                formatName(operationNameSupplier.get(), parentMessage),
                SpanKind.PRODUCER);
        addLinks(spanBuilder, linkedSiblings);
        addMessageAttributes(spanBuilder, parentMessage);
        return new OpenTelemetrySpan(spanBuilder);
    }

    private void addLinks(SpanBuilder spanBuilder, Message<?>[] linkedMessages) {
        for (Message<?> message : linkedMessages) {
            Context linkedContext = textMapPropagator.extract(Context.current(),
                                                              message,
                                                              textMapGetter);
            spanBuilder.addLink(io.opentelemetry.api.trace.Span.fromContext(linkedContext).getSpanContext());
        }
    }

    @Override
    public Span createInternalSpan(Supplier<String> operationNameSupplier) {
        SpanBuilder spanBuilder = createSpanBuilderWithCurrentContext(
                operationNameSupplier.get(),
                SpanKind.INTERNAL
        );
        return new OpenTelemetrySpan(spanBuilder);
    }

    @Override
    public Span createInternalSpan(Supplier<String> operationNameSupplier, Message<?> message) {
        SpanBuilder spanBuilder = createSpanBuilderWithCurrentContext(
                formatName(operationNameSupplier.get(), message),
                SpanKind.INTERNAL
        );
        addMessageAttributes(spanBuilder, message);
        return new OpenTelemetrySpan(spanBuilder);
    }

    @Override
    public void registerSpanAttributeProvider(SpanAttributesProvider provider) {
        spanAttributesProviders.add(provider);
    }

    private String formatName(String operationName, Message<?> message) {
        if (message == null) {
            return operationName;
        }
        return String.format("%s(%s)",
                             operationName,
                             determineMessageName(message));
    }

    private void addMessageAttributes(SpanBuilder spanBuilder, Message<?> message) {
        if (message == null) {
            return;
        }
        spanAttributesProviders.forEach(supplier -> {
            Map<String, String> attributes = supplier.provideForMessage(message);
            attributes.forEach(spanBuilder::setAttribute);
        });
    }

    /**
     * Use the context at moment of creation as parent, not at moment of start. Keep this call, it will break trace
     * correlation otherwise.
     */
    private SpanBuilder createSpanBuilderWithCurrentContext(String name, SpanKind kind) {
        return createSpanBuilder(name, kind).setParent(Context.current());
    }

    private SpanBuilder createSpanBuilder(String name, SpanKind kind) {
        return tracer.spanBuilder(name).setSpanKind(kind);
    }

    /**
     * Builder class to instantiate a {@link OpenTelemetrySpanFactory}.
     * <p>
     * The {@link SpanAttributesProvider SpanAttributeProvieders} are defaulted to an empty list, the {@link Tracer} is
     * defaulted to the tracer defined by {@link GlobalOpenTelemetry}, the {@link TextMapSetter} is defaulted to the
     * {@link MetadataContextSetter} and the {@link TextMapGetter} is defaulted to the {@link MetadataContextGetter}.
     */
    public static class Builder {

        private Tracer tracer = null;
        private TextMapPropagator textMapPropagator = null;
        private TextMapSetter<Map<String, String>> textMapSetter = MetadataContextSetter.INSTANCE;
        private TextMapGetter<Message<?>> textMapGetter = MetadataContextGetter.INSTANCE;

        private final List<SpanAttributesProvider> spanAttributesProviders = new LinkedList<>();

        /**
         * Adds all provided {@link SpanAttributesProvider}s to the {@link SpanFactory}.
         *
         * @param attributesProviders The {@link SpanAttributesProvider}s to add.
         * @return The current Builder instance, for fluent interfacing.
         */
        public Builder addSpanAttributeProviders(@Nonnull List<SpanAttributesProvider> attributesProviders) {
            BuilderUtils.assertNonNull(attributesProviders, "The attributesProviders should not be null");
            spanAttributesProviders.addAll(attributesProviders);
            return this;
        }

        /**
         * Sets the propagator to be used. A propagator is used to propagate the current context into a message, so it
         * can become the parent of another span despite being in another system.
         *
         * @param propagator The propagator to be used.
         * @return The current Builder instance, for fluent interfacing.
         */
        public Builder contextPropagators(TextMapPropagator propagator) {
            this.textMapPropagator = propagator;
            return this;
        }

        /**
         * Defines the {@link Tracer} from OpenTelemetry to use.
         *
         * @param tracer The {@link Tracer} to configure for use.
         * @return The current Builder instance, for fluent interfacing.
         */
        public Builder tracer(@Nonnull Tracer tracer) {
            BuilderUtils.assertNonNull(tracer, "The Tracer should not be null");
            this.tracer = tracer;
            return this;
        }

        /**
         * Defines the {@link TextMapSetter} to use, which is used for propagating the context to another thread or
         * service.
         *
         * @param textMapSetter The {@link TextMapSetter} to configure for use.
         * @return The current Builder instance, for fluent interfacing.
         */
        public Builder textMapSetter(TextMapSetter<Map<String, String>> textMapSetter) {
            BuilderUtils.assertNonNull(textMapSetter, "The TextMapSetter should not be null");
            this.textMapSetter = textMapSetter;
            return this;
        }

        /**
         * Defines the {@link TextMapGetter} to use, which is used for extracting the propagated context from another
         * thread or service.
         *
         * @param textMapGetter The {@link TextMapGetter} to configure for use.
         * @return The current Builder instance, for fluent interfacing.
         */
        public Builder textMapGetter(TextMapGetter<Message<?>> textMapGetter) {
            BuilderUtils.assertNonNull(textMapGetter, "The TextMapGetter should not be null");
            this.textMapGetter = textMapGetter;
            return this;
        }

        /**
         * Initializes the {@link OpenTelemetrySpanFactory}.
         *
         * @return The created {@link OpenTelemetrySpanFactory} with the provided configuration.
         */
        public OpenTelemetrySpanFactory build() {
            if (tracer == null) {
                tracer = GlobalOpenTelemetry.getTracer("AxonFramework-OpenTelemetry");
            }
            if (textMapPropagator == null) {
                textMapPropagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
            }
            return new OpenTelemetrySpanFactory(this);
        }
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/tracing-opentelemetry/src/main/java/org/axonframework/tracing/opentelemetry/OpenTelemetrySpan.java

```java
/*
 * Copyright (c) 2010-2023. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.opentelemetry;

import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import org.axonframework.tracing.Span;
import org.axonframework.tracing.SpanScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * {@link Span} implementation that uses OpenTelemetry's {@link io.opentelemetry.api.trace.Span} to provide tracing
 * capabilities to an application.
 * <p>
 * These traces should always be created using the {@link OpenTelemetrySpanFactory} since this will make sure the proper
 * parent context is extracted before creating the {@link Span}.
 * <p>
 * {@inheritDoc}
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class OpenTelemetrySpan implements Span {

    private static final Logger logger = LoggerFactory.getLogger(OpenTelemetrySpan.class);

    private final SpanBuilder spanBuilder;
    private final List<Scope> scopes = new CopyOnWriteArrayList<>();
    private io.opentelemetry.api.trace.Span span = null;
    private boolean ended = false;

    /**
     * Creates the span, based on the {@link SpanBuilder} provided. This {@link SpanBuilder} will supply the
     * {@link io.opentelemetry.api.trace.Span} when the {@link #start()} method is invoked.
     *
     * @param spanBuilder The provider of the {@link io.opentelemetry.api.trace.Span}.
     */
    public OpenTelemetrySpan(SpanBuilder spanBuilder) {
        Objects.requireNonNull(spanBuilder, "Span builder can not be null!");
        this.spanBuilder = spanBuilder;
    }

    @Override
    public Span start() {
        if (span == null) {
            span = spanBuilder.startSpan();
        } else {
            logger.warn("An attempt was made to start span with id [{}] of trace [{}] a second time",
                         span.getSpanContext().getSpanId(),
                         span.getSpanContext().getTraceId());
        }
        return this;
    }

    @Override
    public SpanScope makeCurrent() {
        if (span == null) {
            logger.warn(
                    "Span was attempted to be made current while not started yet! Please report this to the Axon Framework team.",
                    new IllegalStateException("Span attempted to be made current while not started"));
            // Return empty scope as to not influence user's code
            return () -> {
            };
        }
        Scope scope = span.makeCurrent();
        scopes.add(scope);
        return () -> {
            scopes.remove(scope);
            scope.close();
        };
    }


    @Override
    public void end() {
        if (span == null) {
            logger.warn(
                    "Span was attempted to be ended while not started yet! Please report this to the Axon Framework team.",
                    new IllegalStateException("Span attempted to be ended while not started"));
            return;
        }
        if (ended) {
            logger.warn(
                    "Span ended a second time! Will ignore this ended invocation. Please report this to the Axon Framework team.",
                    new IllegalStateException("Span ended a second time"));
            return;
        }
        if (!scopes.isEmpty()) {
            logger.warn(
                    "Span ended without all scopes! Please report this to the Axon Framework team. This might influence reliability of your OpenTelemetry traces.",
                    new IllegalStateException("Span ended with still " + scopes.size() + " open!"));
        }
        span.end();
        ended = true;
    }

    @Override
    public Span recordException(Throwable t) {
        span.recordException(t);
        span.setStatus(StatusCode.ERROR, t.getMessage());
        return this;
    }

    @Override
    public Span addAttribute(String key, String value) {
        if(this.span == null) {
            spanBuilder.setAttribute(key, value);
        } else {
            span.setAttribute(key, value);
        }
        return this;
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/tracing-opentelemetry/src/main/java/org/axonframework/tracing/opentelemetry/MetadataContextSetter.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.opentelemetry;

import io.opentelemetry.context.propagation.TextMapSetter;
import org.axonframework.messaging.Message;

import java.util.Map;
import javax.annotation.Nonnull;

/**
 * This {@link TextMapSetter} implementation is able to insert the current OpenTelemetry span context into a
 * {@link Message}. However, since a {@code Message} is immutable, this injector injects it into the provided
 * {@link Map}. It's the responsibility the implementing {@link OpenTelemetrySpanFactory} to mutate the message through
 * {@link OpenTelemetrySpanFactory#propagateContext(Message)}.
 * <p>
 * The trace becomes the message's parent span in its{@link org.axonframework.messaging.MetaData}.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class MetadataContextSetter implements TextMapSetter<Map<String, String>> {

    /**
     * Singleton instance of the {@link MetadataContextSetter}, used by the {@link OpenTelemetrySpanFactory}.
     */
    public static final MetadataContextSetter INSTANCE = new MetadataContextSetter();

    private MetadataContextSetter() {
        // Should not be initialized directly, use the public static INSTANCE.
    }

    @Override
    public void set(Map<String, String> metadata, @Nonnull String key, @Nonnull String value) {
        if (metadata == null) {
            throw new IllegalArgumentException("The provided metadata may not be null!");
        }
        metadata.put(key, value);
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/tracing-opentelemetry/src/main/java/org/axonframework/tracing/opentelemetry/MetadataContextGetter.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.opentelemetry;

import io.opentelemetry.context.propagation.TextMapGetter;
import org.axonframework.messaging.Message;

import javax.annotation.Nonnull;

/**
 * This {@link TextMapGetter} implementation is able to extract the parent OpenTelemetry span context from a
 * {@link Message}.
 * <p>
 * The trace parent is part of the message's {@link org.axonframework.messaging.MetaData}, if it was set when
 * dispatching by the {@link MetadataContextSetter}. This is done using the
 * {@link org.axonframework.tracing.SpanFactory#propagateContext(Message)} method for the message.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class MetadataContextGetter implements TextMapGetter<Message<?>> {

    /**
     * Singleton instance of the {@link MetadataContextGetter}, used by the {@link OpenTelemetrySpanFactory}.
     */
    public static final MetadataContextGetter INSTANCE = new MetadataContextGetter();

    private MetadataContextGetter() {
        // Should not be initialized directly, use the public static INSTANCE.
    }

    @Override
    public Iterable<String> keys(Message<?> message) {
        return message.getMetaData().keySet();
    }

    @Override
    public String get(Message<?> message, @Nonnull String key) {
        if (message == null) {
            return null;
        }
        return (String) message.getMetaData().get(key);
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/test/java/org/axonframework/tracing/TestSpanFactory.java

```java
/*
 * Copyright (c) 2010-2024. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

import org.axonframework.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Creates spans that record whether they were started, ended or ended in an exception. Useful for tracing logic
 * assertions in unit tests and provides several out of the box.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class TestSpanFactory implements SpanFactory {

    private final Logger logger = LoggerFactory.getLogger(TestSpanFactory.class);

    private final Deque<TestSpan> activeSpan = new ArrayDeque<>();
    private final List<TestSpan> createdSpans = new CopyOnWriteArrayList<>();
    private final Map<Message<?>, TestSpan> propagatedContexts = new HashMap<>();


    /**
     * Resets the {@link TestSpanFactory} to pristine state.
     */
    public void reset() {
        this.activeSpan.clear();
        this.createdSpans.clear();
        this.propagatedContexts.clear();
        logger.debug("SpanFactory cleared");
    }

    /**
     * Verifies that a span was created, but not started.
     *
     * @param name Name of the span to verify.
     */
    public void verifyNotStarted(String name) {
        assertFalse(findSpan(name, span -> span.started).isPresent(), () -> createErrorMessageForSpan(name));
    }

    /**
     * Verifies that a span was created and started, but it not yet ended.
     *
     * @param name Name of the span to verify.
     */
    public void verifySpanActive(String name) {
        assertTrue(findSpan(name, span -> span.started && !span.ended).isPresent(),
                   () -> createErrorMessageForSpan(name));
    }

    /**
     * Verifies that a span was created and started, but it not yet ended.
     *
     * @param name    Name of the span to verify.
     * @param message The exact message the span should have been created for.
     */
    public void verifySpanActive(String name, Message<?> message) {
        assertTrue(findSpan(name, message, span -> span.started && !span.ended).isPresent(),
                   () -> createErrorMessageForSpan(name));
    }

    /**
     * Verifies that a span was created, started, and ended.
     *
     * @param name Name of the span to verify.
     */
    public void verifySpanCompleted(String name) {
        assertTrue(findSpan(name, span -> span.started && span.ended).isPresent(),
                   () -> createErrorMessageForSpan(name));
    }

    /**
     * Verifies that a Span has a certain attribute set on it.
     * @param name Name of the span to verify.
     * @param key The key of the attribute.
     * @param value The value of the attribute.
     */
    public void verifySpanHasAttributeValue(String name, String key, String value) {
        assertTrue(findSpan(name, span -> span.attributes.containsKey(key) && span.attributes.get(key).equals(value)).isPresent(),
                   () -> createErrorMessageForSpan(name));
    }

    /**
     * Verifies that a span was created, started, and ended.
     *
     * @param name    Name of the span to verify.
     * @param message The exact message the span should have been created for.
     */
    public void verifySpanCompleted(String name, Message<?> message) {
        assertTrue(findSpan(name, message, span -> span.started && span.ended).isPresent(),
                   () -> createErrorMessageForSpan(name));
    }


    /**
     * Verifies that a span had an exception registered to it.
     *
     * @param name           Name of the span to verify.
     * @param exceptionClass The type of exception
     */
    public void verifySpanHasException(String name, Class<?> exceptionClass) {
        assertInstanceOf(exceptionClass, findSpan(name).map(s -> s.exception).orElse(null));
    }

    /**
     * Verify no span was started by this name.
     *
     * @param name Name of the span to verify.
     */
    public void verifyNoSpan(String name) {
        assertFalse(findSpan(name).isPresent());
    }

    /**
     * Verifies the provided message had the context propagated to it.
     *
     * @param name    Name of the span to verify.
     * @param message The exact message the span should have been propagated to.
     */
    public void verifySpanPropagated(String name, Message<?> message) {
        assertTrue(createdSpans.stream()
                               .anyMatch(s -> s.name.equals(name)
                                       && propagatedContexts.containsKey(message)
                                       && propagatedContexts.get(message) == s),
                   createErrorMessageForSpan(name));
    }

    /**
     * Verifies that a span was created and was of a certain type.
     *
     * @param name Name of the span to verify.
     * @see TestSpanType
     */
    public void verifySpanHasType(String name, TestSpanType type) {
        assertEquals(type, findSpan(name).map(s -> s.type).orElse(null));
    }

    private void verifySpanExists(String name) {
        assertTrue(findSpan(name).isPresent(), () -> createErrorMessageForSpan(name));
    }

    private Optional<TestSpan> findSpan(String name) {
        return findSpan(name, testSpan -> true);
    }

    private void verifySpanExists(String name, Predicate<TestSpan> filter) {
        assertTrue(findSpan(name, filter).isPresent(), () -> createErrorMessageForSpan(name));
    }

    private Optional<TestSpan> findSpan(String name, Predicate<TestSpan> filter) {
        return createdSpans.stream()
                           .filter(s -> s.name.equals(name))
                           .filter(filter)
                           .findFirst();
    }

    private String createErrorMessageForSpan(String name) {
        return String.format(
                "No span matching name %s, but got the following recorded spans: %s",
                name,
                createdSpans.stream().map(TestSpan::toString).collect(Collectors.joining("\n")));
    }

    private Optional<TestSpan> findSpan(String name, Message<?> message, Predicate<TestSpan> filter) {
        return findSpan(name, filter.and(
                s -> s.message != null
                        && s.message.getIdentifier().equals(message.getIdentifier())));
    }

    @Override
    public Span createRootTrace(Supplier<String> operationNameSupplier) {
        TestSpan span = new TestSpan(TestSpanType.ROOT, operationNameSupplier.get(), null);
        createdSpans.add(span);
        return span;
    }

    @Override
    public Span createHandlerSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage,
                                  boolean isChildTrace,
                                  Message<?>... linkedParents) {
        TestSpan span = new TestSpan(isChildTrace ? TestSpanType.HANDLER_CHILD : TestSpanType.HANDLER_LINK,
                                     operationNameSupplier.get(),
                                     parentMessage);
        createdSpans.add(span);
        return span;
    }

    @Override
    public Span createDispatchSpan(Supplier<String> operationNameSupplier, Message<?> parentMessage,
                                   Message<?>... linkedSiblings) {
        TestSpan span = new TestSpan(TestSpanType.DISPATCH, operationNameSupplier.get(), parentMessage);
        createdSpans.add(span);
        return span;
    }

    @Override
    public Span createInternalSpan(Supplier<String> operationNameSupplier) {
        TestSpan span = new TestSpan(TestSpanType.INTERNAL, operationNameSupplier.get(), null);
        createdSpans.add(span);
        return span;
    }

    @Override
    public Span createInternalSpan(Supplier<String> operationNameSupplier, Message<?> message) {
        TestSpan span = new TestSpan(TestSpanType.INTERNAL, operationNameSupplier.get(), message);
        createdSpans.add(span);
        return span;
    }

    @Override
    public void registerSpanAttributeProvider(SpanAttributesProvider provider) {

    }

    /**
     * {@inheritDoc}
     * <p>
     * Note that this method does not return a modified message. Lots of testcases rely on an equality check. Modifying
     * the metadata of the message will break this.
     */
    @Override
    public <M extends Message<?>> M propagateContext(M message) {
        synchronized (activeSpan) {
            if (activeSpan.isEmpty()) {
                return message;
            }
            propagatedContexts.put(message, activeSpan.getFirst());
        }
        return message;
    }

    /**
     * The type of the span, relates to the method of {@link SpanFactory}. Used for assertions.
     */
    public enum TestSpanType {
        ROOT,
        HANDLER_CHILD,
        HANDLER_LINK,
        DISPATCH,
        INTERNAL
    }

    /**
     * Implementation of a {@link Span} that registers what happens to it. Other than that, it does not do anything.
     * <p>
     * Starting a span will register it as the current to the {@link TestSpanFactory#activeSpan} deque. All actions are
     * logged to be able to easily debug testcases.
     */
    public class TestSpan implements Span {

        private final List<SpanScope> scopes = new CopyOnWriteArrayList<>();
        private final TestSpanType type;
        private final String name;
        private final Message<?> message;
        private boolean started;
        private boolean ended;
        private Throwable exception;
        private AtomicInteger scopeCount = new AtomicInteger(-1);
        private Map<String, String> attributes = new HashMap<>();

        public TestSpan(TestSpanType type, String name, Message<?> message) {
            this.type = type;
            this.name = name;
            this.message = message;
        }

        @Override
        public Span start() {
            started = true;
            synchronized (activeSpan) {
                activeSpan.addFirst(this);
            }
            logger.debug("+ {}", name);
            return this;
        }

        @Override
        public SpanScope makeCurrent() {
            return new TestSpanScope(scopeCount.incrementAndGet());
        }

        private class TestSpanScope implements SpanScope {

            private final int scopeNum;

            private TestSpanScope(int scopeNum) {
                this.scopeNum = scopeNum;
                logger.debug("++ {}:{}", name, scopeNum);
            }

            @Override
            public void close() {
                logger.debug("-- {}:{}", name, scopeNum);
                scopes.remove(this);
            }
        }

        @Override
        public void end() {
            if(ended) {
                throw new IllegalStateException("Span already ended!");
            }
            ended = true;
            if (scopes.size() > 0) {
                throw new IllegalStateException("All scopes should be closed! Still have " + scopes.size() + " open!");
            }
            synchronized (activeSpan) {
                activeSpan.remove(this);
            }
            logger.debug("- {}", name);
        }

        @Override
        public Span recordException(Throwable t) {
            logger.debug("Recorded exception for span with name {}", name, t);
            this.exception = t;
            return this;
        }

        @Override
        public Span addAttribute(String key, String value) {
            attributes.put(key, value);
            return this;
        }

        public TestSpanType getType() {
            return type;
        }

        public String getName() {
            return name;
        }

        public Map<String, String> getAttributes() {
            return attributes;
        }

        public String getAttribute(String key) {
            return attributes.get(key);
        }

        public Message<?> getMessage() {
            return message;
        }

        @Override
        public String toString() {
            return "TestSpan{" +
                    "type=" + type +
                    ", name='" + name + '\'' +
                    ", message=" + message +
                    ", started=" + started +
                    ", ended=" + ended +
                    ", exception=" + exception +
                    ", attributes=" + attributes +
                    '}';
        }
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/test/java/org/axonframework/tracing/NoOpSpanFactoryTest.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

import org.axonframework.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The {@link NoOpSpanFactory} is not supposed to do anything, but should still provide basic requirements such as
 * returning a non-null span, and the span returning itself in certain situations.
 */
class NoOpSpanFactoryTest {

    @Test
    void createRootTraceReturnsNoOpSpan() {
        Span trace = NoOpSpanFactory.INSTANCE.createRootTrace(() -> "Trace");
        assertTrue(trace instanceof NoOpSpanFactory.NoOpSpan);
    }

    @Test
    void createHandlerSpanReturnsNoOpSpan() {
        Span trace = NoOpSpanFactory.INSTANCE.createHandlerSpan(() -> "Trace", new GenericEventMessage<>("payload"), true);
        assertTrue(trace instanceof NoOpSpanFactory.NoOpSpan);
    }

    @Test
    void createDispatchSpanReturnsNoOpSpan() {
        Span trace = NoOpSpanFactory.INSTANCE.createDispatchSpan(() -> "Trace", new GenericEventMessage<>("payload"));
        assertTrue(trace instanceof NoOpSpanFactory.NoOpSpan);
    }

    @Test
    void createInternalSpanWithMessageReturnsNoOpSpan() {
        Span trace = NoOpSpanFactory.INSTANCE.createInternalSpan(() -> "Trace", new GenericEventMessage<>("payload"));
        assertTrue(trace instanceof NoOpSpanFactory.NoOpSpan);
    }

    @Test
    void createInternalSpanWithoutMessageReturnsNoOpSpan() {
        Span trace = NoOpSpanFactory.INSTANCE.createInternalSpan(() -> "Trace");
        assertTrue(trace instanceof NoOpSpanFactory.NoOpSpan);
    }

    @Test
    void propagateContextReturnsOriginal() {
        GenericEventMessage<String> message = new GenericEventMessage<>("payload");
        GenericEventMessage<String> result = NoOpSpanFactory.INSTANCE.propagateContext(message);
        assertSame(message, result);
    }

    @Test
    void noOpSpanReturnsSelfOnStart() {
        NoOpSpanFactory.NoOpSpan noOpSpan = new NoOpSpanFactory.NoOpSpan();
        assertSame(noOpSpan, noOpSpan.start());
    }

    @Test
    void noOpSpanReturnsSelfOnRecordException() {
        NoOpSpanFactory.NoOpSpan noOpSpan = new NoOpSpanFactory.NoOpSpan();
        assertSame(noOpSpan, noOpSpan.recordException(new RuntimeException("")));
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/test/java/org/axonframework/tracing/MultiSpanFactoryTest.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.messaging.Message;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.Arrays;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MultiSpanFactoryTest {

    private final SpanFactory spanFactory1 = mock(SpanFactory.class);
    private final Span mockSpan1 = mock(Span.class);
    private final SpanFactory spanFactory2 = mock(SpanFactory.class);
    private final Span mockSpan2 = mock(Span.class);
    private final SpanFactory multiSpanFactory = new MultiSpanFactory(Arrays.asList(spanFactory1, spanFactory2));

    private final GenericEventMessage<?> message = new GenericEventMessage<>("payload");
    private final Supplier<String> stringSupplier = () -> "Trace";

    @Test
    void rootTracesCreatedWillDelegateToAllFactories() {
        when(spanFactory1.createRootTrace(any())).thenReturn(mockSpan1);
        when(spanFactory2.createRootTrace(any())).thenReturn(mockSpan2);

        Span span = multiSpanFactory.createRootTrace(() -> "Trace").start();

        Mockito.verify(mockSpan1).start();
        Mockito.verify(mockSpan2).start();

        Mockito.verify(mockSpan1, never()).end();
        Mockito.verify(mockSpan2, never()).end();

        RuntimeException exception = new RuntimeException("My Exception");
        span.recordException(exception).end();

        Mockito.verify(mockSpan1).end();
        Mockito.verify(mockSpan2).end();
        Mockito.verify(mockSpan1).recordException(exception);
        Mockito.verify(mockSpan2).recordException(exception);
    }

    @Test
    void handlerSpansCreatedWillDelegateToAllFactories() {
        multiSpanFactory.createHandlerSpan(stringSupplier, message, false);

        Mockito.verify(spanFactory1).createHandlerSpan(stringSupplier, message, false);
        Mockito.verify(spanFactory2).createHandlerSpan(stringSupplier, message, false);
    }

    @Test
    void dispatchSpansCreatedWillDelegateToAllFactories() {
        multiSpanFactory.createDispatchSpan(stringSupplier, message);

        Mockito.verify(spanFactory1).createDispatchSpan(stringSupplier, message);
        Mockito.verify(spanFactory2).createDispatchSpan(stringSupplier, message);
    }

    @Test
    void internalSpansCreatedWillDelegateToAllFactories() {
        multiSpanFactory.createInternalSpan(stringSupplier);

        Mockito.verify(spanFactory1).createInternalSpan(stringSupplier);
        Mockito.verify(spanFactory2).createInternalSpan(stringSupplier);
    }

    @Test
    void internalSpansWithMessageCreatedWillDelegateToAllFactories() {
        multiSpanFactory.createInternalSpan(stringSupplier, message);

        Mockito.verify(spanFactory1).createInternalSpan(stringSupplier, message);
        Mockito.verify(spanFactory2).createInternalSpan(stringSupplier, message);
    }

    @Test
    void registerSpanAttributeProviderWillDelegateToAllFactories() {
        SpanAttributesProvider provider = mock(SpanAttributesProvider.class);
        multiSpanFactory.registerSpanAttributeProvider(provider);

        Mockito.verify(spanFactory1).registerSpanAttributeProvider(provider);
        Mockito.verify(spanFactory2).registerSpanAttributeProvider(provider);
    }

    @Test
    void propagateContextDelegateToAllFactories() {
        Message original = mock(Message.class);
        Message modifiedFirst = mock(Message.class);
        Message modifiedSecond = mock(Message.class);

        when(spanFactory1.propagateContext(original)).thenReturn(modifiedFirst);
        when(spanFactory2.propagateContext(modifiedFirst)).thenReturn(modifiedSecond);

        Message result = multiSpanFactory.propagateContext(original);
        assertSame(result, modifiedSecond);
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/test/java/org/axonframework/tracing/TracingHandlerEnhancerDefinitionTest.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing;

import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.annotation.MessageHandlingMember;
import org.junit.jupiter.api.*;

import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TracingHandlerEnhancerDefinitionTest {

    private final MessageHandlingMember<TracingHandlerEnhancerDefinitionTest> original = mock(MessageHandlingMember.class);
    private final TestSpanFactory spanFactory = new TestSpanFactory();
    private final Span span = mock(Span.class);

    private boolean invoked = false;

    @BeforeEach
    void setUp() throws Exception {
        when(span.runCallable(any())).thenCallRealMethod();

        Method executable = this.getClass().getDeclaredMethod("executable", MyEvent.class, CommandGateway.class);
        when(original.unwrap(Executable.class)).thenReturn(Optional.of(executable));
    }

    private void setupOriginal(boolean eventSourcingHandler) {
        if (eventSourcingHandler) {
            when(original.attribute("EventSourcingHandler.payloadType")).thenReturn(Optional.of("value"));
        } else {
            when(original.attribute("EventSourcingHandler.payloadType")).thenReturn(Optional.empty());
        }
    }

    @Test
    void showsWhenNotEventSourcingHandler() throws Exception {
        setupOriginal(false);

        TracingHandlerEnhancerDefinition definition = TracingHandlerEnhancerDefinition.builder()
                                                                                      .spanFactory(spanFactory)
                                                                                      .showEventSourcingHandlers(false)
                                                                                      .build();
        MessageHandlingMember<TracingHandlerEnhancerDefinitionTest> messageHandlingMember = definition.wrapHandler(
                original);
        Message<?> message = mock(Message.class);
        when(original.handle(any(), any())).thenAnswer(invocationOnMock -> {
            spanFactory.verifySpanActive("TracingHandlerEnhancerDefinitionTest.executable(MyEvent,CommandGateway)");
            invoked = true;
            return null;
        });
        messageHandlingMember.handle(message, this);

        assertTrue(invoked);
        spanFactory.verifySpanCompleted("TracingHandlerEnhancerDefinitionTest.executable(MyEvent,CommandGateway)");
    }

    @Test
    void doesNotShowWhenEventSourcingHandler() throws Exception {
        setupOriginal(true);

        TracingHandlerEnhancerDefinition definition = TracingHandlerEnhancerDefinition.builder()
                                                                                      .spanFactory(spanFactory)
                                                                                      .showEventSourcingHandlers(false)
                                                                                      .build();
        MessageHandlingMember<TracingHandlerEnhancerDefinitionTest> messageHandlingMember = definition.wrapHandler(
                original);
        assertSame(original, messageHandlingMember);
    }

    @Test
    void showsWhenEventSourcingHandlerButOptionIsTrue() throws Exception {
        setupOriginal(true);

        TracingHandlerEnhancerDefinition definition = TracingHandlerEnhancerDefinition.builder()
                                                                                      .spanFactory(spanFactory)
                                                                                      .showEventSourcingHandlers(true)
                                                                                      .build();
        MessageHandlingMember<TracingHandlerEnhancerDefinitionTest> messageHandlingMember = definition.wrapHandler(
                original);
        Message<?> message = mock(Message.class);
        when(original.handle(any(), any())).thenAnswer(invocationOnMock -> {
            spanFactory.verifySpanActive("TracingHandlerEnhancerDefinitionTest.executable(MyEvent,CommandGateway)");
            invoked = true;
            return null;
        });
        messageHandlingMember.handle(message, this);

        assertTrue(invoked);
        spanFactory.verifySpanCompleted("TracingHandlerEnhancerDefinitionTest.executable(MyEvent,CommandGateway)");
    }

    @Test
    void canNotSetSpanFactoryToNull() {
        TracingHandlerEnhancerDefinition.Builder builder = TracingHandlerEnhancerDefinition.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.spanFactory(null));
    }

    @Test
    void canNotBuildeWithoutSpanFactory() {
        TracingHandlerEnhancerDefinition.Builder builder = TracingHandlerEnhancerDefinition.builder();
        assertThrows(AxonConfigurationException.class, builder::build);
    }

    private class MyEvent {

    }

    private void executable(MyEvent event, CommandGateway commandGateway) {
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/test/java/org/axonframework/commandhandling/DefaultCommandBusSpanFactoryTest.java

```java
/*
 * Copyright (c) 2010-2023. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.commandhandling;

import org.axonframework.tracing.IntermediateSpanFactoryTest;
import org.axonframework.tracing.SpanFactory;
import org.axonframework.tracing.TestSpanFactory;
import org.junit.jupiter.api.*;

class DefaultCommandBusSpanFactoryTest
        extends IntermediateSpanFactoryTest<DefaultCommandBusSpanFactory.Builder, DefaultCommandBusSpanFactory> {

    @Test
    void createLocalDispatchCommand() {
        CommandMessage<Object> command = GenericCommandMessage.asCommandMessage("MyCommand");
        test(spanFactory -> spanFactory.createDispatchCommandSpan(command, false),
             expectedSpan("CommandBus.dispatchCommand", TestSpanFactory.TestSpanType.INTERNAL)
                     .withMessage(command)
        );
    }

    @Test
    void createDistributedDispatchCommand() {
        CommandMessage<Object> command = GenericCommandMessage.asCommandMessage("MyCommand");
        test(spanFactory -> spanFactory.createDispatchCommandSpan(command, true),
             expectedSpan("CommandBus.dispatchDistributedCommand", TestSpanFactory.TestSpanType.DISPATCH)
                     .withMessage(command)
        );
    }


    @Test
    void createLocalHandleCommand() {
        CommandMessage<Object> command = GenericCommandMessage.asCommandMessage("MyCommand");
        test(spanFactory -> spanFactory.createHandleCommandSpan(command, false),
             expectedSpan("CommandBus.handleCommand", TestSpanFactory.TestSpanType.HANDLER_CHILD)
                     .withMessage(command)
        );
    }

    @Test
    void createDistributedHandleCommandDefault() {
        CommandMessage<Object> command = GenericCommandMessage.asCommandMessage("MyCommand");
        test(spanFactory -> spanFactory.createHandleCommandSpan(command, true),
             expectedSpan("CommandBus.handleDistributedCommand", TestSpanFactory.TestSpanType.HANDLER_CHILD)
                     .withMessage(command)
        );
    }

    @Test
    void createDistributedHandleCommandDistributedNotSameTrace() {
        CommandMessage<Object> command = GenericCommandMessage.asCommandMessage("MyCommand");
        test(builder -> builder.distributedInSameTrace(false),
             spanFactory -> spanFactory.createHandleCommandSpan(command, true),
             expectedSpan("CommandBus.handleDistributedCommand", TestSpanFactory.TestSpanType.HANDLER_LINK)
                     .withMessage(command)
        );
    }

    @Test
    void propagateContext() {
        CommandMessage<Object> command = GenericCommandMessage.asCommandMessage("MyCommand");
        testContextPropagation(command, DefaultCommandBusSpanFactory::propagateContext);
    }

    @Override
    protected DefaultCommandBusSpanFactory.Builder createBuilder(SpanFactory spanFactory) {
        return DefaultCommandBusSpanFactory.builder().spanFactory(spanFactory);
    }

    @Override
    protected DefaultCommandBusSpanFactory createFactoryBasedOnBuilder(DefaultCommandBusSpanFactory.Builder builder) {
        return builder.build();
    }
}
```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/tracing-opentelemetry/src/test/java/org/axonframework/tracing/opentelemetry/OpenTelemetrySpanFactoryTest.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.opentelemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.messaging.Message;
import org.axonframework.tracing.SpanAttributesProvider;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OpenTelemetrySpanFactoryTest {

    private final static OpenTelemetry otelMock = mock(OpenTelemetry.class);
    private final Tracer tracer = mock(Tracer.class);
    private final TextMapPropagator textMapPropagator = mock(TextMapPropagator.class);
    private final SpanAttributesProvider spanAttributesProvider = mock(SpanAttributesProvider.class);
    private final SpanBuilder spanBuilder = mock(SpanBuilder.class);
    private final ContextKey<Object> parentKey = ContextKey.named("opentelemetry-trace-span-key");

    private OpenTelemetrySpanFactory factory;

    @BeforeEach
    void setUp() {
        // We need to mock out a lot of the OTEL functionality. Without java agent instrumentation, it only uses a NOOP
        // variant, which is useless for the scope of these tests.
        when(otelMock.getTracer(any())).thenReturn(tracer);
        when(otelMock.getPropagators()).thenReturn(() -> textMapPropagator);
        doAnswer(invocationOnMock -> {
            invocationOnMock.getArgument(2, MetadataContextSetter.class)
                            .set(invocationOnMock.getArgument(1), "traceparent", "MY_TRACE_PARENT");
            return null;
        }).when(textMapPropagator).inject(any(Context.class), any(), eq(MetadataContextSetter.INSTANCE));

        doAnswer(invocationOnMock -> {
            MetadataContextGetter getter = invocationOnMock.getArgument(2, MetadataContextGetter.class);
            Message message = invocationOnMock.getArgument(1, Message.class);
            Context context = invocationOnMock.getArgument(0, Context.class);
            return context.with(parentKey, getter.get(message, "traceparent"));
        }).when(textMapPropagator).extract(any(Context.class), any(Message.class), eq(MetadataContextGetter.INSTANCE));

        // Mock out span builder methods
        when(spanBuilder.setSpanKind(any())).thenReturn(spanBuilder);
        when(spanBuilder.addLink(any())).thenReturn(spanBuilder);
        when(spanBuilder.setNoParent()).thenReturn(spanBuilder);
        when(spanBuilder.setParent(any())).thenReturn(spanBuilder);
        when(spanBuilder.setAttribute(anyString(), any())).thenReturn(spanBuilder);
        when(tracer.spanBuilder(anyString())).thenReturn(spanBuilder);

        factory = OpenTelemetrySpanFactory.builder()
                                          .tracer(tracer)
                                          .addSpanAttributeProviders(Collections.singletonList(spanAttributesProvider))
                                          .textMapGetter(MetadataContextGetter.INSTANCE)
                                          .textMapSetter(MetadataContextSetter.INSTANCE)
                                          .build();
    }

    @BeforeAll
    static void beforeAll() {
        GlobalOpenTelemetry.set(otelMock);
    }

    @Test
    void propagatesContextInjectsMetadata() {
        EventMessage<Object> originalMessage = GenericEventMessage.asEventMessage("MyEvent");
        EventMessage<Object> modifiedMessage = factory.propagateContext(originalMessage);

        assertNotNull(modifiedMessage.getMetaData().get("traceparent"));
    }

    @Test
    void createRootTracesCreatesSpanWithNoParentLinkedToCurrent() {
        SpanContext spanContext = Span.current().getSpanContext();
        org.axonframework.tracing.Span span = factory.createRootTrace(() -> "MyRootTrace");

        verify(spanBuilder).setNoParent();
        verify(spanBuilder).addLink(spanContext);
        verify(spanBuilder).setSpanKind(SpanKind.INTERNAL);
    }

    @Test
    void createHandlerSpanExtractsParentContext() {
        Message<?> message = generateMessageWithTraceId("1");
        factory.createChildHandlerSpan(() -> "MyRootTrace", message);

        ArgumentCaptor<Context> parentCaptor = ArgumentCaptor.forClass(Context.class);
        verify(spanBuilder).setParent(parentCaptor.capture());
        verify(spanBuilder).setSpanKind(SpanKind.CONSUMER);
        assertEquals("1", parentCaptor.getValue().get(parentKey));
    }

    @Test
    void createHandlerSpanAddsLinks() {
        Message<?> message = generateMessageWithTraceId("1");
        factory.createChildHandlerSpan(() -> "MyRootTrace",
                                       message,
                                       generateMessageWithTraceId("2"),
                                       generateMessageWithTraceId("3"));

        verify(spanBuilder).setParent(any());
        verify(spanBuilder).setSpanKind(SpanKind.CONSUMER);
        verify(spanBuilder, times(2)).addLink(any());
    }

    @Test
    void createLinkedHandlerSpanExtractsLinkedContext() {
        Message<?> message = generateMessageWithTraceId("1");
        factory.createLinkedHandlerSpan(() -> "MyRootTrace", message);

        verify(spanBuilder, times(1)).addLink(any());
        verify(spanBuilder).setSpanKind(SpanKind.CONSUMER);
        verify(spanBuilder).setNoParent();
    }

    @Test
    void createHandlerSpanAddsAttributes() {
        Message<?> message = generateMessageWithTraceId("1");
        when(spanAttributesProvider.provideForMessage(any())).thenReturn(Collections.singletonMap("myKey", "myValue"));
        factory.createLinkedHandlerSpan(() -> "MyRootTrace", message);

        verify(spanBuilder).setAttribute("myKey", "myValue");
    }

    @Test
    void createDispatchSpanAddsLinks() {
        Message<?> message = generateMessageWithTraceId("1");
        factory.createDispatchSpan(() -> "MyRootTrace",
                                   message,
                                   generateMessageWithTraceId("2"),
                                   generateMessageWithTraceId("3"));

        verify(spanBuilder).setSpanKind(SpanKind.PRODUCER);
        verify(spanBuilder, times(2)).addLink(any());
    }

    @Test
    void createDispatchSpanSetsCurrentContextAsParent() {
        Message<?> message = generateMessageWithTraceId("1");
        factory.createDispatchSpan(() -> "MyRootTrace",
                                   message,
                                   generateMessageWithTraceId("2"),
                                   generateMessageWithTraceId("3"));

        verify(spanBuilder).setParent(Context.current());
    }

    @Test
    void createDispatchSpanAddsAttributes() {
        Message<?> message = generateMessageWithTraceId("1");
        when(spanAttributesProvider.provideForMessage(any())).thenReturn(Collections.singletonMap("myKey", "myValue"));
        factory.createDispatchSpan(() -> "MyRootTrace", message);

        verify(spanBuilder).setAttribute("myKey", "myValue");
    }

    @Test
    void createInternalSpanWithoutMessage() {
        factory.createInternalSpan(() -> "MyRootTrace");

        verify(spanBuilder).setSpanKind(SpanKind.INTERNAL);
    }

    @Test
    void createInternalSpanSetsCurrentContextAsParent() {
        factory.createInternalSpan(() -> "MyRootTrace");

        verify(spanBuilder).setParent(Context.current());
    }

    @Test
    void createInternalSpanAddsAttributes() {
        Message<?> message = generateMessageWithTraceId("1");
        when(spanAttributesProvider.provideForMessage(any())).thenReturn(Collections.singletonMap("myKey", "myValue"));
        factory.createInternalSpan(() -> "MyRootTrace", message);

        verify(spanBuilder).setSpanKind(SpanKind.INTERNAL);
        verify(spanBuilder).setAttribute("myKey", "myValue");
    }

    private Message<?> generateMessageWithTraceId(String traceId) {
        return GenericEventMessage.asEventMessage("MyEvent")
                                  .andMetaData(Collections.singletonMap("traceparent", traceId));
    }

    @Test
    void builderRejectsNullTracer() {
        OpenTelemetrySpanFactory.Builder builder = OpenTelemetrySpanFactory.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.tracer(null));
    }

    @Test
    void builderRejectsNullSpanAttributeProviders() {
        OpenTelemetrySpanFactory.Builder builder = OpenTelemetrySpanFactory.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.addSpanAttributeProviders(null));
    }

    @Test
    void builderRejectsNullTextMapGetter() {
        OpenTelemetrySpanFactory.Builder builder = OpenTelemetrySpanFactory.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.textMapGetter(null));
    }

    @Test
    void builderRejectsNullTextMapSetter() {
        OpenTelemetrySpanFactory.Builder builder = OpenTelemetrySpanFactory.builder();
        assertThrows(AxonConfigurationException.class, () -> builder.textMapSetter(null));
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/tracing-opentelemetry/src/test/java/org/axonframework/tracing/opentelemetry/OpenTelemetrySpanTest.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.opentelemetry;

import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import org.axonframework.tracing.Span;
import org.axonframework.tracing.SpanScope;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OpenTelemetrySpanTest {

    private final SpanBuilder spanBuilder = Mockito.mock(SpanBuilder.class);
    private final io.opentelemetry.api.trace.Span span = Mockito.mock(io.opentelemetry.api.trace.Span.class);
    private final List<Scope> openScopeList = new CopyOnWriteArrayList<>();

    private OpenTelemetrySpan openTelemetrySpan;

    @BeforeEach
    void setUp() {
        when(spanBuilder.startSpan()).thenReturn(span);
        when(span.makeCurrent()).thenAnswer(invocation -> {
            Scope mock = mock(Scope.class);
            openScopeList.add(mock);
            doAnswer((invocationOnMock) -> {
                openScopeList.remove(mock);
                return null;
            }).when(mock).close();
            return mock;
        });
        when(span.getSpanContext()).thenReturn(mock(SpanContext.class));

        openTelemetrySpan = new OpenTelemetrySpan(spanBuilder);
    }

    @Test
    void startingTheSpanStartsItButDoesNotMakeItCurrent() {
        openTelemetrySpan.start();

        verify(spanBuilder).startSpan();
        assertEquals(0, openScopeList.size());
    }

    @Test
    void startingTheSpanAndMakingItActiveMakesItCurrent() {
        openTelemetrySpan.start();
        SpanScope scope = openTelemetrySpan.makeCurrent();

        verify(spanBuilder).startSpan();
        verify(span).makeCurrent();

        assertEquals(1, openScopeList.size());
        scope.close();
        assertEquals(0, openScopeList.size());
    }

    @Test
    void endingTheSpanDoesNotCloseOpenScopes() {
        Span start = openTelemetrySpan.start();
        start.makeCurrent();

        start.end();
        verify(openScopeList.get(0), never()).close();
        verify(span).end();
    }

    @Test
    void endsSpanEventWhenSpansAreStillCurrent() {
        Span axonSpan = openTelemetrySpan.start();
        axonSpan.start();
        axonSpan.makeCurrent();

        axonSpan.end();
        verify(span, times(1)).end();
    }

    @Test
    void recordsExceptionsOnSpan() {
        IllegalArgumentException exception = new IllegalArgumentException("This is my exception message");
        openTelemetrySpan.start().recordException(exception);

        verify(span).recordException(exception);
        verify(span).setStatus(StatusCode.ERROR, "This is my exception message");
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/test/java/org/axonframework/tracing/attributes/MessageIdSpanAttributesProviderTest.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.attributes;

import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.messaging.Message;
import org.axonframework.tracing.SpanAttributesProvider;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageIdSpanAttributesProviderTest {

    private final SpanAttributesProvider provider = new MessageIdSpanAttributesProvider();

    @Test
    void extractsMessageIdentifier() {
        Message<?> event = GenericEventMessage.asEventMessage("Some event");
        Map<String, String> map = provider.provideForMessage(event);
        assertEquals(1, map.size());
        assertEquals(event.getIdentifier(), map.get("axon_message_id"));
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/test/java/org/axonframework/tracing/attributes/MessageNameSpanAttributesProviderTest.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.attributes;

import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.responsetypes.ResponseTypes;
import org.axonframework.queryhandling.GenericQueryMessage;
import org.axonframework.tracing.SpanAttributesProvider;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageNameSpanAttributesProviderTest {

    private final SpanAttributesProvider provider = new MessageNameSpanAttributesProvider();

    @Test
    void extractsNothingForEvent() {
        EventMessage<Object> event = GenericEventMessage.asEventMessage("Some event");
        Map<String, String> map = provider.provideForMessage(event);
        assertEquals(0, map.size());
    }

    @Test
    void extractsForQueryWithSpecificName() {
        Message<?> genericQueryMessage = new GenericQueryMessage<>("MyQuery",
                                                                   "myQueryName",
                                                                   ResponseTypes.instanceOf(String.class));
        Map<String, String> map = provider.provideForMessage(genericQueryMessage);
        assertEquals(1, map.size());
        assertEquals("myQueryName", map.get("axon_message_name"));
    }

    @Test
    void extractsForQueryWithPayloadName() {
        Message<?> genericQueryMessage = new GenericQueryMessage<>("MyQuery",
                                                                   ResponseTypes.instanceOf(String.class));
        Map<String, String> map = provider.provideForMessage(genericQueryMessage);
        assertEquals(1, map.size());
        assertEquals("java.lang.String", map.get("axon_message_name"));
    }

    @Test
    void extractsForCommandWithSpecificName() {
        Message<?> genericQueryMessage = new GenericCommandMessage<>(new GenericCommandMessage<>("payload"),
                                                                     "MyAwesomeCommand");
        Map<String, String> map = provider.provideForMessage(genericQueryMessage);
        assertEquals(1, map.size());
        assertEquals("MyAwesomeCommand", map.get("axon_message_name"));
    }

    @Test
    void extractsForCommandWithPayloadName() {
        Message<?> genericQueryMessage = new GenericCommandMessage<>("payload");
        Map<String, String> map = provider.provideForMessage(genericQueryMessage);
        assertEquals(1, map.size());
        assertEquals("java.lang.String", map.get("axon_message_name"));
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/test/java/org/axonframework/tracing/attributes/MessageTypeSpanAttributesProviderTest.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.attributes;

import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.responsetypes.ResponseTypes;
import org.axonframework.queryhandling.GenericQueryMessage;
import org.axonframework.tracing.SpanAttributesProvider;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageTypeSpanAttributesProviderTest {

    private final SpanAttributesProvider provider = new MessageTypeSpanAttributesProvider();

    @Test
    void correctTypeForQueryMessage() {
        Message<?> genericQueryMessage = new GenericQueryMessage<>("MyQuery",
                                                                 "myQueryName",
                                                                 ResponseTypes.instanceOf(String.class));
        Map<String, String> map = provider.provideForMessage(genericQueryMessage);
        assertEquals(1, map.size());
        assertEquals("GenericQueryMessage", map.get("axon_message_type"));
    }

    @Test
    void correctTypeForCommandMessage() {
        Message<?> genericQueryMessage = new GenericCommandMessage<>("payload");
        Map<String, String> map = provider.provideForMessage(genericQueryMessage);
        assertEquals(1, map.size());
        assertEquals("GenericCommandMessage", map.get("axon_message_type"));
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/test/java/org/axonframework/tracing/attributes/PayloadTypeSpanAttributesProviderTest.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.attributes;

import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.messaging.Message;
import org.axonframework.tracing.SpanAttributesProvider;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PayloadTypeSpanAttributesProviderTest {

    private final SpanAttributesProvider provider = new PayloadTypeSpanAttributesProvider();

    @Test
    void stringPayload() {
        Message<?> message = new GenericEventMessage<>("MyEvent");

        Map<String, String> map = provider.provideForMessage(message);
        assertEquals(1, map.size());
        assertEquals("java.lang.String", map.get("axon_payload_type"));
    }

    @Test
    void classPayload() {
        Message<?> message = new GenericEventMessage<>(new MyEvent());

        Map<String, String> map = provider.provideForMessage(message);
        assertEquals(1, map.size());
        assertEquals("org.axonframework.tracing.attributes.PayloadTypeSpanAttributesProviderTest$MyEvent",
                     map.get("axon_payload_type"));
    }

    private static class MyEvent {

    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/test/java/org/axonframework/tracing/attributes/MetadataSpanAttributesProviderTest.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.attributes;

import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.messaging.Message;
import org.axonframework.tracing.SpanAttributesProvider;
import org.junit.jupiter.api.*;

import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.*;

class MetadataSpanAttributesProviderTest {

    private final SpanAttributesProvider provider = new MetadataSpanAttributesProvider();

    @Test
    void addsAllMetadata() {
        Message<?> message = new GenericEventMessage<>("MyEvent")
                .andMetaData(singletonMap("myKeyOne", "valueOne"))
                .andMetaData(singletonMap("myNumberKey", 2))
                .andMetaData(singletonMap("someOtherKey_2", "someValue"));
        Map<String, String> map = provider.provideForMessage(message);
        assertEquals(3, map.size());
        assertEquals("valueOne", map.get("axon_metadata_myKeyOne"));
        assertEquals("2", map.get("axon_metadata_myNumberKey"));
        assertEquals("someValue", map.get("axon_metadata_someOtherKey_2"));
    }
}

```

## /Users/mateusznowak/GitRepos/AxonFramework/AxonFramework4/messaging/src/test/java/org/axonframework/tracing/attributes/AggregateIdentifierSpanAttributesProviderTest.java

```java
/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.tracing.attributes;

import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.messaging.Message;
import org.axonframework.tracing.SpanAttributesProvider;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AggregateIdentifierSpanAttributesProviderTest {

    private final SpanAttributesProvider provider = new AggregateIdentifierSpanAttributesProvider();

    @Test
    void domainEventMessage() {
        Message<?> message = new GenericDomainEventMessage<>("MyType", "1729872981", 1, "payload");

        Map<String, String> map = provider.provideForMessage(message);
        assertEquals(1, map.size());
        assertEquals("1729872981", map.get("axon_aggregate_identifier"));
    }
    @Test
    void genericEventMessage() {
        Message<?> message = new GenericEventMessage<>("payload");

        Map<String, String> map = provider.provideForMessage(message);
        assertEquals(0, map.size());
    }
}

```

