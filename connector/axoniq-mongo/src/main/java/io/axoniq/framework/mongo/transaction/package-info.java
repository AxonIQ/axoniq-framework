/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

/**
 * Transaction support for the MongoDB components: a {@link com.mongodb.client.ClientSession ClientSession}-based
 * {@link org.axonframework.common.tx.TransactionalExecutor TransactionalExecutor}, its provider, and a
 * {@link org.axonframework.messaging.core.unitofwork.transaction.TransactionManager TransactionManager} that binds them
 * to a {@link org.axonframework.messaging.core.unitofwork.ProcessingContext ProcessingContext}.
 */
@NullMarked
package io.axoniq.framework.mongo.transaction;

import org.jspecify.annotations.NullMarked;