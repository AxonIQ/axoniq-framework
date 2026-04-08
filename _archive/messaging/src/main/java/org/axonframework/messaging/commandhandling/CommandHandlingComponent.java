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

package org.axonframework.messaging.commandhandling;

import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.QualifiedName;

import java.util.Set;

/**
 * Interface describing a group of {@link CommandHandler CommandHandlers} belonging to a single component. It specifies
 * the {@link #supportedCommands() commands} it supports.
 *
 * @author Allard Buijze
 * @author Rene de Waele
 * @author Steven van Beelen
 * @since 3.0.0
 */
public interface CommandHandlingComponent extends CommandHandler, DescribableComponent {

    /**
     * All supported {@link CommandMessage commands}, referenced through a {@link QualifiedName}.
     *
     * @return All supported {@link CommandMessage commands}, referenced through a {@link QualifiedName}.
     */
    Set<QualifiedName> supportedCommands();
}
