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

package io.axoniq.framework.messaging.multitenancy.api;

import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageHandlerInterceptorChain;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class RegisterTenantDescriptorHandlerInterceptorTest {

    private static final TenantDescriptor TENANT_A = new TenantDescriptor(
            "foo-a",
            Map.of("replicationGroup", "rg-a")
    );

    @Nested
    class ConstructorWithTenantResolver {

        @Test
        void registersTheResolvedTenantDescriptorBeforeProceedingForCommandMessages() {
            // given
            RegisterTenantDescriptorHandlerInterceptor testSubject =
                    new RegisterTenantDescriptorHandlerInterceptor(new MetadataBasedTenantResolver("lateTenantKey"));
            Message message = new GenericCommandMessage(
                    new MessageType("TestCommand"),
                    "payload",
                    Map.of("lateTenantKey", TENANT_A.tenantId())
            );
            ProcessingContext context = StubProcessingContext.forMessage(message);
            CapturingChain interceptorChain = new CapturingChain();

            // when
            MessageStream<?> result = testSubject.interceptOnHandle(message, context, interceptorChain);

            // then
            assertThat(result).isSameAs(interceptorChain.result);
            assertThat(TenantDescriptor.fromContext(interceptorChain.context))
                    .contains(TenantDescriptor.tenantWithId(TENANT_A.tenantId()));
        }

        @Test
        void initializesTenantDescriptorsToAnEmptyList() {
            // given
            RegisterTenantDescriptorHandlerInterceptor testSubject =
                    new RegisterTenantDescriptorHandlerInterceptor(new MetadataBasedTenantResolver("lateTenantKey"));
            Message message = new GenericCommandMessage(
                    new MessageType("TestCommand"),
                    "payload",
                    Map.of("lateTenantKey", TENANT_A.tenantId())
            );
            TenantDescriptor tenantDescriptor = new MetadataBasedTenantResolver("lateTenantKey").resolveTenant(message);
            ProcessingContext context = StubProcessingContext.forMessage(message)
                    .withResource(TenantDescriptor.RESOURCE_KEY, tenantDescriptor);
            CapturingChain interceptorChain = new CapturingChain();

            // when
            testSubject.interceptOnHandle(message, context, interceptorChain);

            // then
            assertThat(testSubject.tenantDescriptors().tenants()).isEmpty();
            assertThat(TenantDescriptor.fromContext(interceptorChain.context))
                    .contains(TenantDescriptor.tenantWithId(TENANT_A.tenantId()));
        }

        @Test
        void proceedsWithoutTenantContextWhenTheTenantCannotBeResolved() {
            // given
            RegisterTenantDescriptorHandlerInterceptor testSubject =
                    new RegisterTenantDescriptorHandlerInterceptor(new MetadataBasedTenantResolver("lateTenantKey"));
            Message message = new GenericCommandMessage(new MessageType("TestCommand"), "payload");
            ProcessingContext context = StubProcessingContext.forMessage(message);
            CapturingChain interceptorChain = new CapturingChain();

            // when
            MessageStream<?> result = testSubject.interceptOnHandle(message, context, interceptorChain);

            // then
            assertThat(result).isSameAs(interceptorChain.result);
            assertThat(TenantDescriptor.fromContext(interceptorChain.context)).isEmpty();
        }

        @Test
        void skipsResolvingTheTenantForNonCommandAndNonQueryMessages() {
            // given
            TenantResolver tenantResolver = mock(TenantResolver.class);
            when(tenantResolver.resolveTenant(any(Message.class), any())).thenReturn(TENANT_A);

            RegisterTenantDescriptorHandlerInterceptor testSubject =
                    new RegisterTenantDescriptorHandlerInterceptor(tenantResolver);
            Message message = new GenericMessage("message-id",
                                                 new MessageType("TestEvent"),
                                                 "payload".getBytes(),
                                                 Map.of("lateTenantKey", TENANT_A.tenantId()));
            ProcessingContext context = StubProcessingContext.forMessage(message);
            CapturingChain interceptorChain = new CapturingChain();

            // when
            MessageStream<?> result = testSubject.interceptOnHandle(message, context, interceptorChain);

            // then
            assertThat(result).isSameAs(interceptorChain.result);
            verify(tenantResolver, never()).resolveTenant(any(Message.class), any());
            assertThat(interceptorChain.context).isSameAs(context);

            assertThat(TenantDescriptor.fromContext(interceptorChain.context)).isEmpty();
        }
    }

    private static class CapturingChain implements MessageHandlerInterceptorChain<Message> {

        private final MessageStream<?> result = MessageStream.empty();
        private ProcessingContext context;

        @Override
        @NonNull
        public MessageStream<?> proceed(@NonNull Message message, @NonNull ProcessingContext context) {
            this.context = context;
            return result;
        }
    }
}
