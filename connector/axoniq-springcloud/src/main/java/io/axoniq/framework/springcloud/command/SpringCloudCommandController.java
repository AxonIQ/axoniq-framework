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

package io.axoniq.framework.springcloud.command;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Receives commands sent by other members of the cluster over HTTP.
 * <p>
 * Every member must expose this endpoint under the same path, since that is how members reach each other; the path is
 * the value of the {@code axon.springcloud.command-endpoint} property, defaulting to
 * {@link #DEFAULT_COMMAND_ENDPOINT}. This controller is registered by the Spring Boot autoconfiguration.
 * <p>
 * Returning a {@link CompletableFuture} puts the request into Spring MVC's asynchronous handling, releasing the
 * container thread while the command is processed on the command bus's own executor. That matters because a member
 * receiving commands has no reason to tie up one container thread per command in flight.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
@RestController
@RequestMapping("${axon.springcloud.command-endpoint:" + SpringCloudCommandController.DEFAULT_COMMAND_ENDPOINT + "}")
public class SpringCloudCommandController {

    /**
     * The path commands are received under when none is configured.
     */
    public static final String DEFAULT_COMMAND_ENDPOINT = "/axoniq-springcloud/command";

    private final IncomingCommandInvoker invoker;

    /**
     * Constructs a {@code SpringCloudCommandController} handing received commands to the given {@code invoker}.
     *
     * @param invoker the component invoking this application's local command handler
     */
    public SpringCloudCommandController(IncomingCommandInvoker invoker) {
        this.invoker = Objects.requireNonNull(invoker, "The invoker must not be null.");
    }

    /**
     * Handles the given {@code request}, sent by another member of the cluster.
     * <p>
     * Answers with {@code 200 OK} whether handling succeeded or failed; a failure is reported in the body of the
     * reply. See {@link CommandDispatchReply} for why.
     *
     * @param request the command sent by another member
     * @return a future completing with the outcome of handling the command
     */
    @PostMapping
    public CompletableFuture<CommandDispatchReply> receiveCommand(@RequestBody CommandDispatchRequest request) {
        return invoker.handle(request);
    }
}
