/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.transaction;

import com.mongodb.client.ClientSession;
import org.axonframework.common.function.ThrowingFunction;
import org.axonframework.common.tx.MaterializationAware;
import org.axonframework.common.tx.MaterializingResource;
import org.axonframework.common.tx.TransactionalExecutor;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A {@link TransactionalExecutor} implementation for MongoDB {@link ClientSession ClientSessions}.
 * <p>
 * The session is obtained from the given {@code sessionSupplier} at most once, the first time {@link #apply} is called,
 * and reused for the remainder of this instance's lifetime. Concurrent {@link #apply} calls are serialized, since a
 * {@code ClientSession} is not safe for concurrent use.
 * <p>
 * This executor never commits, aborts, or closes the session itself; that remains the responsibility of whoever
 * supplied it. This is deliberate: a session reached through this class is the one bound to an externally managed unit
 * of work, such as the one the {@link ClientSessionTransactionManager} controls.
 *
 * @author Steven van Beelen
 * @since 5.5.0
 */
public class ClientSessionExecutor implements TransactionalExecutor<ClientSession>, MaterializationAware {

    private final MaterializingResource<ClientSession> delegate;

    /**
     * Creates a new instance that obtains its {@link ClientSession} from the given {@code sessionSupplier}.
     *
     * @param sessionSupplier the supplier of the {@link ClientSession}, invoked at most once
     */
    public ClientSessionExecutor(Callable<ClientSession> sessionSupplier) {
        Objects.requireNonNull(sessionSupplier, "sessionSupplier");
        this.delegate = new MaterializingResource<>(sessionSupplier);
    }

    @Override
    public <R> R withMaterializationShielded(Function<Boolean, R> decision) {
        return delegate.withMaterializationShielded(decision);
    }

    @Override
    public <R> CompletableFuture<R> apply(ThrowingFunction<ClientSession, R, Exception> function) {
        return delegate.apply(function);
    }
}
