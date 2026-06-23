/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
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

package io.axoniq.framework.examples.cli;

import picocli.CommandLine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Logs are written to file so the REPL is not polluted with log messages.
 * using this command, we can still see the log messages, without having to open the log file.
 */
@CommandLine.Command(name = "log", description = "Display the configured log file.")
public final class LogCommand extends SubCommand {

    @Override
    public void run() {
        Path logFile = resolveLogFile();
        if (!Files.exists(logFile)) {
            echo("Log file does not exist: " + logFile);
            return;
        }

        echo("=== " + logFile + " ===");
        try {
            List<String> lines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
            if (lines.isEmpty()) {
                echo("(log file is empty)");
                return;
            }
            lines.forEach(this::echo);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read log file [" + logFile + "].", e);
        }
    }

    static Path resolveLogFile() {
        String configuredLogFile = System.getProperty("LOG_FILE");
        if (configuredLogFile == null || configuredLogFile.isBlank()) {
            configuredLogFile = System.getenv("LOG_FILE");
        }
        if (configuredLogFile == null || configuredLogFile.isBlank()) {
            throw new IllegalStateException("LOG_FILE is not configured.");
        }
        return Path.of(configuredLogFile);
    }
}
