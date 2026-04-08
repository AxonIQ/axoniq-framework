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

package org.axonframework.modelling.entity.annotation;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.modelling.entity.child.CommandTargetResolver;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.*;
import org.mockito.junit.jupiter.*;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link RoutingKeyCommandTargetResolverDefinition}.
 *
 * @author Mitchell Herrijgers
 */
@ExtendWith(MockitoExtension.class)
class RoutingKeyCommandTargetResolverDefinitionTest {

    private final CommandTargetResolverDefinition definition = new RoutingKeyCommandTargetResolverDefinition();

    @Mock
    private AnnotatedEntityMetamodel<ChildEntity> childEntityMetamodel;

    @Test
    void allowsNoRoutingKeyOnSingleValueEntityMember() throws NoSuchFieldException {
        CommandTargetResolver<ChildEntity> result = definition.createCommandTargetResolver(
                childEntityMetamodel,
                SingleChildEntity.class.getDeclaredField("child")
        );

        assertThat(result).isEqualTo(CommandTargetResolver.MATCH_ANY());
    }

    @Test
    void doesNotAllowMissingRoutingKeyOnCollectionTypeMember() {
        assertThatThrownBy(
                () -> definition.createCommandTargetResolver(
                        childEntityMetamodel,
                        ListChildEntityWithoutRoutingKey.class.getDeclaredField("child")
                ),
                "Expected AxonConfigurationException when no routing key is present on a collection type member"
        ).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void returnsMatcherForListChildEntityWithRoutingKey() throws NoSuchFieldException {
        CommandTargetResolver<ChildEntity> result = definition.createCommandTargetResolver(
                childEntityMetamodel,
                ListChildEntityWithRoutingKey.class.getDeclaredField("child")
        );

        assertThat(result).isNotNull()
                          .isNotEqualTo(CommandTargetResolver.MATCH_ANY());
    }

    @Test
    void doesNotAllowRoutingKeyMismatchBetweenMessageAndEntity() throws NoSuchFieldException {
        CommandTargetResolver<ChildEntity> result = definition.createCommandTargetResolver(
                childEntityMetamodel,
                ListChildEntityWithNonMatchingRoutingKey.class.getDeclaredField("child")
        );

        MessageType messageType = new MessageType(TestCommand.class);
        //noinspection unchecked,rawtypes
        when(childEntityMetamodel.getExpectedRepresentation(messageType.qualifiedName()))
                .thenReturn((Class) TestCommand.class);

        assertThatThrownBy(() -> result.getTargetChildEntity(
                List.of(new ChildEntity()),
                new GenericCommandMessage(messageType, new TestCommand("someValue")),
                new StubProcessingContext()
        )).isInstanceOf(UnknownRoutingKeyException.class);
    }

    static class SingleChildEntity {

        @SuppressWarnings("unused")
        @EntityMember
        private ChildEntity child;
    }

    static class ListChildEntityWithoutRoutingKey {

        @SuppressWarnings("unused")
        @EntityMember
        private List<ChildEntity> child;
    }

    static class ListChildEntityWithRoutingKey {

        @SuppressWarnings("unused")
        @EntityMember(routingKey = "key")
        private List<ChildEntity> child;
    }

    static class ListChildEntityWithNonMatchingRoutingKey {

        @SuppressWarnings("unused")
        @EntityMember(routingKey = "incorrect")
        private List<ChildEntity> child;
    }

    static class ChildEntity {

        @SuppressWarnings("unused")
        String key() {
            return "key";
        }
    }

    private record TestCommand(String key) {

    }
}
