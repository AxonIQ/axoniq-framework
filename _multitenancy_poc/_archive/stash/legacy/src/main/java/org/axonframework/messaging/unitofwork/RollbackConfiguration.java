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

package org.axonframework.messaging.unitofwork;

/**
 * The RollbackConfiguration defines if a Unit of Work should be rolled back when an exception is raised during the
 * processing of a Message.
 * <p/>
 * Note that the Unit of Work will always throw any exceptions raised during processing, regardless of whether or not
 * the Unit of Work is rolled back.
 *
 * @author Martin Tilma
 * @since 1.1
 */
@Deprecated // exceptions need to be handled by an interceptor, or otherwise they are always considered an error
public interface RollbackConfiguration {

    /**
     * Decides whether the given {@code throwable} should trigger a rollback.
     *
     * @param throwable the Throwable to evaluate
     * @return {@code true} if the UnitOfWork should be rolled back, {@code false} otherwise
     */
    boolean rollBackOn(Throwable throwable);
}
