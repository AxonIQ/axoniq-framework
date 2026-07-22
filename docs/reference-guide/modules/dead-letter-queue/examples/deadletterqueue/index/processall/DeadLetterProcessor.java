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

package deadletterqueue.index.processall;

// tag::process-all[]
import org.axonframework.common.configuration.Configuration;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterProcessor;
import java.util.concurrent.TimeUnit;

public class DeadLetterProcessor {

    private final Configuration configuration;

    // end::process-all[]
    DeadLetterProcessor(Configuration configuration) {
        this.configuration = configuration;
    }

    // tag::process-all[]
    public void retryAllSequences(String processorName, String componentName) {
        // dead letter processor names follow the pattern `EventHandlingComponent[" + processorName + "][" + componentName + "]`
        var dlqEhc = "EventHandlingComponent[" + processorName + "][" + componentName + "]";
        configuration.getModuleConfiguration(processorName)
                .flatMap(m -> m.getOptionalComponent(SequencedDeadLetterProcessor.class, dlqEhc))
                .ifPresent(this::processUntilEmpty);
    }

    private void processUntilEmpty(SequencedDeadLetterProcessor<?> dlp) {
        boolean processed = true;
        while (processed) {
            processed = dlp.processAny()
                           .orTimeout(30, TimeUnit.SECONDS)
                           .join();
        }
    }
}
// end::process-all[]
