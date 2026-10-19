/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.deadletter;

// tag::replay-for-tenant[]
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterProcessor;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.concurrent.CompletableFuture;

public class TenantDeadLetterReplay {

    private final SequencedDeadLetterProcessor<EventMessage> processor;
    private final UnitOfWorkFactory uowFactory;

    public TenantDeadLetterReplay(SequencedDeadLetterProcessor<EventMessage> processor,
                                  UnitOfWorkFactory uowFactory) {
        this.processor = processor;
        this.uowFactory = uowFactory;
    }

    public CompletableFuture<Boolean> processAnyLetterFor(String tenantId) {
        return uowFactory.create().executeWithResult(context -> {
            context.putResource(TenantDescriptor.RESOURCE_KEY, TenantDescriptor.tenantWithId(tenantId));
            return processor.processAny(context);
        });
    }
}
// end::replay-for-tenant[]
