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

package org.axonframework.messaging.unitofwork;

/**
 * Enum containing common rollback configurations for the Unit of Work.
 *
 * @author Rene de Waele
 * @since 3.0
 */
public enum RollbackConfigurationType implements RollbackConfiguration {

    /**
     * Configuration that never performs a rollback of the unit of work.
     */
    NEVER {
        @Override
        public boolean rollBackOn(Throwable throwable) {
            return false;
        }
    },

    /**
     * Configuration that prescribes a rollback on any sort of exception or error.
     */
    ANY_THROWABLE {
        @Override
        public boolean rollBackOn(Throwable throwable) {
            return true;
        }
    },

    /**
     * Configuration that prescribes a rollback on any sort of unchecked exception, including errors.
     */
    UNCHECKED_EXCEPTIONS {
        @Override
        public boolean rollBackOn(Throwable throwable) {
            return !(throwable instanceof Exception) || throwable instanceof RuntimeException;
        }
    },

    /**
     * Configuration that prescribes a rollback on runtime exceptions only.
     */
    RUNTIME_EXCEPTIONS {
        @Override
        public boolean rollBackOn(Throwable throwable) {
            return throwable instanceof RuntimeException;
        }
    }

}
