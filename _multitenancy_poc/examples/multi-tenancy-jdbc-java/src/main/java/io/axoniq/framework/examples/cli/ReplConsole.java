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

package io.axoniq.framework.examples.cli;

import org.axonframework.common.configuration.AxonConfiguration;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;
import org.jline.reader.impl.DefaultParser;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

@Command(
        name = "university",
        mixinStandardHelpOptions = true,
        description = "Interactive REPL for the university example.",
        subcommands = {
                DescribeCommand.class,
                RepoCommand.class,
                SendCommand.class,
                SendQuery.class,
                LogCommand.class,
                ReplConsole.HelpCommand.class,
                ReplConsole.ExitCommand.class
        }
)
public final class ReplConsole implements Runnable {

    final AxonConfiguration axonConfiguration;
    private final CommandLine commandLine;
    private final Parser parser = new DefaultParser();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Scanner input;
    private final PrintWriter output;

    public ReplConsole(AxonConfiguration axonConfiguration, InputStream input, OutputStream output) {
        this.axonConfiguration = requireNonNull(axonConfiguration, "The axonConfiguration cannot be null.");
        this.input = new Scanner(requireNonNull(input, "The input cannot be null."),
                                 StandardCharsets.UTF_8);
        this.output = new PrintWriter(requireNonNull(output, "The output cannot be null."),
                                      true);
        this.commandLine = init();
    }

    private CommandLine init() {
        var commandLine = new CommandLine(this);
        commandLine.setOut(this.output);
        commandLine.setErr(this.output);
        commandLine.setExecutionExceptionHandler((exception, cl, parseResult) -> {
            this.output.println(exception.getMessage());
            return cl.getCommandSpec().exitCodeOnExecutionException();
        });
        return commandLine;
    }

    @Override
    public void run() {
        output.println("University JDBC REPL started. Type 'help' to list commands and 'exit' to stop.");
        while (running.get()) {
            output.print("> ");
            output.flush();
            if (!input.hasNextLine()) {
                break;
            }
            String line = input.nextLine().trim();
            if (line.isBlank()) {
                continue;
            }
            try {
                ParsedLine parsed = parser.parse(line, line.length());
                commandLine.execute(parsed.words().toArray(String[]::new));
            } catch (CommandLine.ParameterException e) {
                output.println(e.getMessage());
            } catch (SyntaxError e) {
                output.println(e.getMessage());
            }
        }
    }

    public PrintWriter output() {
        return output;
    }

    public CommandLine commandLine() {
        return commandLine;
    }

    public void requestStop() {
        running.set(false);
    }

    @CommandLine.Command(name = "help", description = "Show the available REPL commands.")
    public static final class HelpCommand implements Runnable {

        @CommandLine.ParentCommand
        private ReplConsole repl;

        @Override
        public void run() {
            repl.commandLine().usage(repl.output());
        }
    }

    @CommandLine.Command(name = "exit", aliases = {"quit"}, description = "Stop the REPL.")
    public static final class ExitCommand extends SubCommand{

        @Override
        public void run() {
            repl.requestStop();
            echo("Bye.");
        }
    }

}
