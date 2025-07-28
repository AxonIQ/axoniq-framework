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

package io.axoniq.console.framework.api;

public enum AxoniqConsoleDlqMode {
    /**
     * All data of messages in the DLQ are available in the API.
     */
    FULL,
    /**
     * In this mode the event payload will never send over the wire. From the diagnostics only the entries which are
     * included in the whitelist will be sent over the wire. The sequence identifier will not be hashed and send as-is.
     */
    LIMITED,
    /**
     * Limited data of messages in the DLQ are available in the API. Sensitive information is masked.
     * The sequence identifier is hashed through SHA256 to still enable processing actions.
     */
    MASKED,
    /**
     * No DLQ data is available in the API. Only the count in the overview. List actions will return an empty list.
     */
    NONE,
}
