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

package io.axoniq.framework.integrationtests.testsuite.administration;

import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.integrationtests.testsuite.administration.AbstractAdministrationIT;
import org.axonframework.integrationtests.testsuite.administration.common.PersonIdentifier;
import org.axonframework.integrationtests.testsuite.administration.state.mutable.MutablePerson;
import org.axonframework.modelling.entity.EntityMetamodel;

/**
 * Runs the administration test suite using as many reflection components of the {@link EntityMetamodel} and related
 * classes as possible. As reflection-based components are added, this test may change to use more of them.
 */
public class MutableReflectionEntityModelAdministrationIT extends AbstractAdministrationIT {

    @Override
    protected EventSourcingConfigurer testSuiteConfigurer(EventSourcingConfigurer configurer) {
        var personEntity = EventSourcedEntityModule.autodetected(PersonIdentifier.class, MutablePerson.class);
        return configurer.componentRegistry(cr -> cr.registerModule(personEntity));
    }

}
