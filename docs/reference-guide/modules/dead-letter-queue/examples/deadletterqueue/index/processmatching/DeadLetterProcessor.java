/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package deadletterqueue.index.processmatching;

// tag::process-matching[]
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.QualifiedName;
import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterProcessor;
import org.axonframework.messaging.eventhandling.EventMessage;
import java.util.concurrent.TimeUnit;

public class DeadLetterProcessor {

    private static final QualifiedName ERROR_EVENT_NAME =
            new QualifiedName("YourApplicationEvent");

    private final Configuration configuration;

    // end::process-matching[]
    DeadLetterProcessor(Configuration configuration) {
        this.configuration = configuration;
    }

    // tag::process-matching[]
    public void retryErrorEventSequence(String processorName, String componentName) {
        // the event processor's module is registered under `EventProcessor[" + processorName + "]`
        var eventProcessorModule = "EventProcessor[" + processorName + "]";
        // dead letter processor names follow the pattern `EventHandlingComponent[" + processorName + "][" + componentName + "]`
        var dlqEhc = "EventHandlingComponent[" + processorName + "][" + componentName + "]";
        configuration.getModuleConfiguration(eventProcessorModule)
                .flatMap(m -> m.getOptionalComponent(SequencedDeadLetterProcessor.class, dlqEhc))
                .ifPresent(dlp ->
                        dlp.process(letter -> ((DeadLetter<? extends EventMessage>) letter).message()
                                                    .type()
                                                    .qualifiedName()
                                                    .equals(ERROR_EVENT_NAME))
                           .orTimeout(30, TimeUnit.SECONDS)
                           .join());
    }
}
// end::process-matching[]
