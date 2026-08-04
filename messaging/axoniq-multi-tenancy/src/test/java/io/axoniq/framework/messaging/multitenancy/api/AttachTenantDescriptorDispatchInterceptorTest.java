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
import org.axonframework.messaging.core.MessageDispatchInterceptorChain;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AttachTenantDescriptorDispatchInterceptorTest {

    private static final String TENANT_KEY = "MyTenantKey";
    private static final TenantDescriptor TENANT_A = new TenantDescriptor(
            "foo-a",
            Map.of("replicationGroup", "rg-a")
    );

    @Nested
    class InterceptOnDispatch {

        @Test
        void attachesTheContextResolvedTenantToCommandMessagesBeforeProceeding() {
            // given
            AttachTenantDescriptorDispatchInterceptor testSubject =
                    new AttachTenantDescriptorDispatchInterceptor(routerKnowing(TENANT_A));
            Message message = new GenericCommandMessage(new MessageType("TestCommand"), "payload");
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);
            CapturingChain interceptorChain = new CapturingChain();

            // when
            MessageStream<?> result = testSubject.interceptOnDispatch(message, context, interceptorChain);

            // then
            assertThat(result).isSameAs(interceptorChain.result);
            assertThat(interceptorChain.message.metadata().get(TENANT_KEY)).isEqualTo(TENANT_A.tenantId());
        }

        @Test
        void overwritesTheMessageAttachedTenantWithTheContextResolvedTenantOnCommandMessagesBeforeProceeding() {
            // given
            AttachTenantDescriptorDispatchInterceptor testSubject =
                    new AttachTenantDescriptorDispatchInterceptor(routerKnowing(TENANT_A,
                                                                                TenantDescriptor.tenantWithId(
                                                                                        "myCustomTenant")));
            Message message = new GenericCommandMessage(new MessageType("TestCommand"), "payload")
                    .andMetadata(Map.of(TENANT_KEY, "myCustomTenant"));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);
            CapturingChain interceptorChain = new CapturingChain();

            // when
            MessageStream<?> result = testSubject.interceptOnDispatch(message, context, interceptorChain);

            // then
            assertThat(result).isSameAs(interceptorChain.result);
            assertThat(interceptorChain.message.metadata().get(TENANT_KEY)).isEqualTo(TENANT_A.tenantId());
        }

        @Test
        void attachesTheContextResolvedTenantToQueryMessagesBeforeProceeding() {
            // given
            AttachTenantDescriptorDispatchInterceptor testSubject =
                    new AttachTenantDescriptorDispatchInterceptor(routerKnowing(TENANT_A));
            Message message = new GenericQueryMessage(new GenericMessage("message-id",
                                                                         new MessageType("TestQuery"),
                                                                         "payload".getBytes(),
                                                                         Map.of()));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);
            CapturingChain interceptorChain = new CapturingChain();

            // when
            MessageStream<?> result = testSubject.interceptOnDispatch(message, context, interceptorChain);

            // then
            assertThat(result).isSameAs(interceptorChain.result);
            assertThat(interceptorChain.message.metadata().get(TENANT_KEY)).isEqualTo(TENANT_A.tenantId());
        }

        @Test
        void overwritesTheMessageAttachedTenantWithTheContextResolvedTenantOnQueryMessagesBeforeProceeding() {
            // given
            AttachTenantDescriptorDispatchInterceptor testSubject =
                    new AttachTenantDescriptorDispatchInterceptor(routerKnowing(TENANT_A,
                                                                                TenantDescriptor.tenantWithId(
                                                                                        "myCustomTenant")));
            Message message = new GenericQueryMessage(new GenericMessage("message-id",
                                                                         new MessageType("TestQuery"),
                                                                         "payload".getBytes(),
                                                                         Map.of()))
                    .andMetadata(Map.of(TENANT_KEY, "myCustomTenant"));
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);
            CapturingChain interceptorChain = new CapturingChain();

            // when
            MessageStream<?> result = testSubject.interceptOnDispatch(message, context, interceptorChain);

            // then
            assertThat(result).isSameAs(interceptorChain.result);
            assertThat(interceptorChain.message.metadata().get(TENANT_KEY)).isEqualTo(TENANT_A.tenantId());
        }

        @Test
        void proceedsUnmodifiedWhenTheContextIsNull() {
            // given
            AttachTenantDescriptorDispatchInterceptor testSubject =
                    new AttachTenantDescriptorDispatchInterceptor(routerKnowing(TENANT_A));
            Message message = new GenericCommandMessage(new MessageType("TestCommand"), "payload");
            CapturingChain interceptorChain = new CapturingChain();

            // when
            MessageStream<?> result = testSubject.interceptOnDispatch(message, null, interceptorChain);

            // then
            assertThat(result).isSameAs(interceptorChain.result);
            assertThat(interceptorChain.message).isSameAs(message);
        }

        @Test
        void proceedsUnmodifiedWhenNoTenantResolvesFromTheContext() {
            // given
            AttachTenantDescriptorDispatchInterceptor testSubject =
                    new AttachTenantDescriptorDispatchInterceptor(routerKnowing(TENANT_A));
            Message message = new GenericCommandMessage(new MessageType("TestCommand"), "payload");
            ProcessingContext context = new StubProcessingContext();
            CapturingChain interceptorChain = new CapturingChain();

            // when
            MessageStream<?> result = testSubject.interceptOnDispatch(message, context, interceptorChain);

            // then
            assertThat(result).isSameAs(interceptorChain.result);
            assertThat(interceptorChain.message).isSameAs(message);
        }

        @Test
        void proceedsUnmodifiedForMessageTypesThatAreNeitherCommandsNorQueries() {
            // given
            AttachTenantDescriptorDispatchInterceptor testSubject =
                    new AttachTenantDescriptorDispatchInterceptor(routerKnowing(TENANT_A));
            Message message = new GenericEventMessage(new MessageType("TestEvent"), "payload", Map.of());
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TenantDescriptor.RESOURCE_KEY, TENANT_A);
            CapturingChain interceptorChain = new CapturingChain();

            // when
            MessageStream<?> result = testSubject.interceptOnDispatch(message, context, interceptorChain);

            // then
            assertThat(result).isSameAs(interceptorChain.result);
            assertThat(interceptorChain.message).isSameAs(message);
        }

        @Test
        void propagatesTenantNotResolvedExceptionWhenTheContextCarriesAnUnknownTenant() {
            // given
            AttachTenantDescriptorDispatchInterceptor testSubject =
                    new AttachTenantDescriptorDispatchInterceptor(routerKnowing(TENANT_A));
            Message message = new GenericCommandMessage(new MessageType("TestCommand"), "payload");
            ProcessingContext context = new StubProcessingContext()
                    .withResource(TenantDescriptor.RESOURCE_KEY, TenantDescriptor.tenantWithId("unknown-tenant"));
            CapturingChain interceptorChain = new CapturingChain();

            // when / then
            assertThatThrownBy(() -> testSubject.interceptOnDispatch(message, context, interceptorChain))
                    .isInstanceOf(TenantNotResolvedException.class)
                    .hasMessageContaining("unknown-tenant");
        }
    }

    private static TenantRouter routerKnowing(TenantDescriptor... tenants) {
        return new TenantRouter(new MetadataBasedTenantResolver(TENANT_KEY), () -> List.of(tenants));
    }

    private static class CapturingChain implements MessageDispatchInterceptorChain<Message> {

        private final MessageStream<?> result = MessageStream.empty();
        private Message message;

        @Override
        public MessageStream<?> proceed(Message message, @Nullable ProcessingContext context) {
            this.message = message;
            return result;
        }
    }
}
