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

package org.axonframework.modelling.command.inspection;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.modelling.command.AggregateMember;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the use the {@link AnnotatedAggregateMetaModelFactory} using the {@link
 * org.axonframework.modelling.command.AggregateMember} annotation. For the time being more specifically through using
 * this annotation with a collection of members, where the type is an interface.
 *
 * @author Steven van Beelen
 */
class AnnotatedAggregateAggregateMemberMetaModelFactoryTest {

    @Test
    void interfaceAggregateMemberThrowsAxonConfigurationException() {
        assertThrows(
                AxonConfigurationException.class,
                () -> AnnotatedAggregateMetaModelFactory.inspectAggregate(TestAggregate.class)
        );
    }

    @Test
    void interfaceAggregateMemberCollectionThrowsAxonConfigurationException() {
        assertThrows(
                AxonConfigurationException.class,
                () -> AnnotatedAggregateMetaModelFactory.inspectAggregate(TestCollectionAggregate.class)
        );
    }

    @Test
    void interfaceAggregateMemberMapThrowsAxonConfigurationException() {
        assertThrows(
                AxonConfigurationException.class,
                () -> AnnotatedAggregateMetaModelFactory.inspectAggregate(TestMapAggregate.class)
        );
    }

    @SuppressWarnings("unused")
    private static class TestAggregate {

        @AggregateMember
        private AggregateMemberInterface aggregateMember;
    }

    @SuppressWarnings("unused")
    private static class TestCollectionAggregate {

        @AggregateMember
        private List<AggregateMemberInterface> aggregateMembers;
    }

    @SuppressWarnings("unused")
    private static class TestMapAggregate {

        @AggregateMember
        private Map<String, AggregateMemberInterface> aggregateMembers;
    }

    private interface AggregateMemberInterface {

        @CommandHandler
        void handle(InterfaceMemberCommand command);
    }

    @SuppressWarnings("unused")
    private static class AggregateMemberImplementationOne implements AggregateMemberInterface {

        @Override
        public void handle(InterfaceMemberCommand command) {

        }

        @CommandHandler
        public void handle(MemberOneCommand command) {

        }
    }

    @SuppressWarnings("unused")
    private static class AggregateMemberImplementationTwo implements AggregateMemberInterface {

        @Override
        public void handle(InterfaceMemberCommand command) {

        }

        @CommandHandler
        public void handle(MemberTwoCommand command) {

        }
    }

    private static class InterfaceMemberCommand {

    }

    private static class MemberOneCommand {

    }

    private static class MemberTwoCommand {

    }
}
