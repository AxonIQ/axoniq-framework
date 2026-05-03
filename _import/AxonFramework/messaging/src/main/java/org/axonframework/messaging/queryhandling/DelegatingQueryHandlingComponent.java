/*
 * Copyright (c) 2010-2026. Axon Framework
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

package org.axonframework.messaging.queryhandling;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Objects;
import java.util.Set;

/**
 * Abstract implementation of a {@link QueryHandlingComponent} that delegates all calls to a given delegate.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public abstract class DelegatingQueryHandlingComponent implements QueryHandlingComponent {

    protected final QueryHandlingComponent delegate;

    /**
     * Constructs the component with the given {@code delegate} to receive calls.
     *
     * @param delegate the instance to delegate calls to
     */
    public DelegatingQueryHandlingComponent(QueryHandlingComponent delegate) {
        this.delegate = Objects.requireNonNull(delegate, "Delegate QueryHandlingComponent may not be null");
    }

    @Override
    public MessageStream<QueryResponseMessage> handle(QueryMessage query, ProcessingContext context) {
        return delegate.handle(query, context);
    }

    @Override
    public Set<QualifiedName> supportedQueries() {
        return delegate.supportedQueries();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
    }
}
