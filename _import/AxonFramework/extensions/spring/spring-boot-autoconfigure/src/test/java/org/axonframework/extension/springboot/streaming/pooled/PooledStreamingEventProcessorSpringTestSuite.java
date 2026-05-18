/*
 * Copyright (c) 2010-2026. Axon Framework
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

package org.axonframework.extension.springboot.streaming.pooled;

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorTestSuite;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

/**
 * Spring Boot variant of {@link PooledStreamingEventProcessorTestSuite}.
 * <p>
 * Obtains the {@link AxonConfiguration} from the Spring {@link ApplicationContext}.
 *
 * @since 5.1.1
 */
public abstract class PooledStreamingEventProcessorSpringTestSuite extends PooledStreamingEventProcessorTestSuite {

    @Autowired
    private AxonConfiguration axonConfiguration;

    @Override
    protected AxonConfiguration axonConfiguration() {
        return axonConfiguration;
    }
}
