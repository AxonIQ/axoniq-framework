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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */

package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.PayloadProcessor;
import io.axoniq.workflow.runtime.api.PrimitiveCommands;
import io.axoniq.workflow.runtime.api.WorkflowStepResult;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Map;

public class BlockingExecuteWithResultCommand<T> extends PrimitiveCommands.DelegatingExecuteCommand<T> {

    private final Converter converter;
    private final TypeReference<T> type;

    public BlockingExecuteWithResultCommand(
            PrimitiveCommands.WorkflowStepResultExecuteCommand command,
            Converter converter,
            TypeReference<T> type
    ) {
        super(command);
        this.converter = converter;
        this.type = type;
    }

    public static <T> BlockingExecuteWithResultCommand<T> blockingLocal(
            String stepName, Map<String, Object> payload, PayloadProcessor action, Duration timeout,
            TypeReference<T> type, Converter converter
    ) {
        return new BlockingExecuteWithResultCommand<>(
                PrimitiveCommands.localExecute(stepName, payload, action, timeout),
                converter,
                type
        );
    }

    @Override
    public T result(@NotNull WorkflowStepResult result) {
        if (result.isSuccess() && result.<Map<String, Object>>result().isPresent()) {
            Map<String, Object> resultPayload = result.<Map<String, Object>>result().get();
            return converter.convert(resultPayload, type.getType());
        } else {
            throw result.error().orElseThrow();
        }
    }
}
