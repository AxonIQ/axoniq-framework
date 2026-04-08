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

package org.axonframework.update;

import org.axonframework.update.api.UpdateCheckRequest;
import org.axonframework.update.api.UpdateCheckResponse;

/**
 * Interface for reporting the response of the update checker to the user. Implementations of this interface should
 * handle how the response is communicated to the user.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public interface UpdateCheckerReporter {

    /**
     * Reports the given {@code response} of the update checker to the user.
     *
     * @param request  The request that was made to the update checker.
     * @param response The response to report.
     */
    void report(UpdateCheckRequest request, UpdateCheckResponse response);
}
