/*
 * Copyright (c) 2022-2025. AxonIQ B.V.
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

package io.axoniq.platform.framework.api;

public enum DomainEventAccessMode {
    /**
     * Full access: payload is visible and loading domain state is supported.
     */
    FULL,

    /**
     * Payload is hidden (e.g., masked), but loading domain state is still supported.
     */
    LOAD_DOMAIN_STATE_ONLY,

    /**
     * Payload is visible, but loading domain state is not supported.
     */
    PREVIEW_PAYLOAD_ONLY,

    /**
     * No access: payload is hidden and loading domain state is not supported.
     */
    NONE
}