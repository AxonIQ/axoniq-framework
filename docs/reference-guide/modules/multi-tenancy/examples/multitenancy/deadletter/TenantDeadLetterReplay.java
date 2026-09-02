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

package multitenancy.deadletter;

import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterProcessor;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.concurrent.CompletableFuture;

public class TenantDeadLetterReplay {

    private final SequencedDeadLetterProcessor<EventMessage> processor;
    private final UnitOfWorkFactory jdbcUnitOfWorkFactory;

    public TenantDeadLetterReplay(SequencedDeadLetterProcessor<EventMessage> processor,
                                  UnitOfWorkFactory jdbcUnitOfWorkFactory) {
        this.processor = processor;
        this.jdbcUnitOfWorkFactory = jdbcUnitOfWorkFactory;
    }

    // tag::replay-for-tenant[]
    public CompletableFuture<Boolean> replayOldestSequence(String tenantId) {
        return jdbcUnitOfWorkFactory.create().executeWithResult(context -> {
            context.putResource(TenantDescriptor.RESOURCE_KEY, TenantDescriptor.tenantWithId(tenantId));
            return processor.processAny(context);
        });
    }
    // end::replay-for-tenant[]
}
