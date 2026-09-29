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

package io.axoniq.framework.testcontainer;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.axonframework.common.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.awaitility.Awaitility.await;


/**
 * Utility class for the {@link AxonServerContainer}, used to initialize the cluster.
 *
 * @author Milan Savic
 * @author Sara Pelligrini
 * @since 4.8.0
 */
public class AxonServerContainerUtils {

    /**
     * Constant {@code boolean} specifying that a context supports DCB.
     */
    public static final boolean DCB_CONTEXT = true;
    /**
     * Constant {@code boolean} specifying that a context does <b>not</b> support DCB.
     */
    public static final boolean NO_DCB_CONTEXT = false;

    /**
     * Connect/read timeout for every HTTP call in this class, in milliseconds.
     * <p>
     * {@link HttpURLConnection} defaults to no timeout at all, so a shared Axon Server instance that accepts a
     * connection but stops responding (e.g. under CPU/disk pressure on a busy CI runner) would otherwise hang the
     * calling thread indefinitely -- observed in practice as a build silently stuck until the CI job's own timeout
     * killed it, rather than the test failing (and retrying) promptly.
     */
    private static final int HTTP_TIMEOUT_MILLIS = 10_000;

    /**
     * Initialize the cluster of the Axon Server instance located at the given {@code hostname} and {@code port}
     * combination.
     * <p>
     * Note that this constructs the contexts {@code _admin} and {@code default}.
     *
     * @param hostname       The hostname of the Axon Server instance to initiate the cluster for.
     * @param port           The port of the Axon Server instance to initiate the cluster for.
     * @param shouldBeReused If set to {@code true}, ensure the cluster is not accidentally initialized twice.
     * @param dcbContext A {@code boolean} stating whether a DCB or non-DCB context is being created.
     * @throws IOException When there are issues with the HTTP connection to the Axon Server instance at the given
     *                     {@code hostname} and {@code port}.
     */
    public static void initCluster(String hostname, int port, boolean shouldBeReused, boolean dcbContext) throws IOException {
        if (shouldBeReused && initialized(hostname, port)) {
            return;
        }
        final URL url = URI.create(String.format("http://%s:%d/v2/cluster/init?dcb=%s", hostname, port, dcbContext)).toURL();
        HttpURLConnection connection = null;
        try {
            connection = openConnection(url);
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            connection.getInputStream().close();

            int responseCode = connection.getResponseCode();
            Assert.isTrue(202 == responseCode, () -> "The response code [" + responseCode + "] did not match 202.");
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        waitForContextsCondition(
                hostname, port,
                contexts -> contexts.contains("_admin") && contexts.contains("default")
        );
    }

    /**
     * Retrieves all the contexts of the Axon Server instance located at the given {@code hostname} and {@code port}
     * combination.
     *
     * @param hostname The hostname of the Axon Server instance to create the given {@code context} of.
     * @param port     The port of the Axon Server instance to create the given {@code context} of.
     * @return All the contexts of the Axon Server instances located at the given {@code hostname} and {@code port}
     * combination.
     * @throws IOException When there are issues with the HTTP connection to the Axon Server instance at the given
     *                     {@code hostname} and {@code port}.
     */
    public static List<String> contexts(String hostname, int port) throws IOException {
        final URL url = URI.create(String.format("http://%s:%d/v1/public/context", hostname, port)).toURL();
        HttpURLConnection connection = null;
        try {
            connection = openConnection(url);
            connection.setRequestProperty("Accept", "application/json");
            connection.setDoOutput(true);
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();
            Assert.isTrue(200 == responseCode, () -> "The response code [" + responseCode + "] did not match 200.");

            return contexts(connection);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * Retrieves all the internal contexts (registered in the RAFT protocol) of the Axon Server instance located at the
     * given {@code hostname} and {@code port} combination.
     *
     * @param hostname The hostname of the Axon Server instance to create the given {@code context} of.
     * @param port     The port of the Axon Server instance to create the given {@code context} of.
     * @return All the contexts of the Axon Server instances located at the given {@code hostname} and {@code port}
     * combination.
     * @throws IOException When there are issues with the HTTP connection to the Axon Server instance at the given
     *                     {@code hostname} and {@code port}.
     */
    public static List<String> internalContexts(String hostname, int port) throws IOException {
        final URL url = new URL(String.format("http://%s:%d/internal/raft/contexts", hostname, port));
        HttpURLConnection connection = null;
        try {
            connection = openConnection(url);
            connection.setRequestProperty("Accept", "application/json");
            connection.setDoOutput(true);
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();
            Assert.isTrue(200 == responseCode, () -> "The response code [" + responseCode + "] did not match 200.");

            return contexts(connection);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static List<String> contexts(HttpURLConnection connection) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        StringBuilder output = new StringBuilder();
        String outputLine;
        while ((outputLine = br.readLine()) != null) {
            output.append(outputLine);
        }
        JsonElement jsonElement = JsonParser.parseString(output.toString());
        ArrayList<String> contexts = new ArrayList<>();
        for (JsonElement element : jsonElement.getAsJsonArray()) {
            String context = element.getAsJsonObject()
                                    .get("context")
                                    .getAsString();
            contexts.add(context);
        }
        return contexts;
    }

    private static void waitForContextsCondition(String hostname,
                                                 int port,
                                                 Predicate<List<String>> condition) {
        await().atMost(Duration.ofMinutes(1))
               .pollInterval(100, TimeUnit.MILLISECONDS)
               .ignoreExceptionsInstanceOf(IOException.class)
               .until(() -> condition.test(internalContexts(hostname, port)));
    }

    private static boolean initialized(String hostname, int port) throws IOException {
        try {
            List<String> cont = internalContexts(hostname, port);
            return cont.contains("_admin") && cont.contains("default");
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Calls the API of Axon Server at given {@code hostname} and (http) {@code port} to purge events of the given
     * {@code context}.
     *
     * @param hostname   The hostname where AxonServer can be reached.
     * @param port       The HTTP port AxonServer listens to for API calls.
     * @param context    The context to purge.
     * @param dcbContext A {@code boolean} stating whether a DCB or non-DCB context is being purged.
     * @throws IOException When an error occurs communicating with Axon Server.
     * @since 5.0.0
     */
    public static void purgeEventsFromAxonServer(String hostname,
                                                 int port,
                                                 String context,
                                                 boolean dcbContext) throws IOException {
        purgeEventsFromAxonServer(hostname, port, context, dcbContext, context);
    }

    /**
     * Calls the API of Axon Server at given {@code hostname} and (http) {@code port} to purge events of the given
     * {@code context}.
     *
     * @param hostname         The hostname where AxonServer can be reached.
     * @param port             The HTTP port AxonServer listens to for API calls.
     * @param context          The context to purge.
     * @param dcbContext       A {@code boolean} stating whether a DCB or non-DCB context is being purged.
     * @param replicationGroup The replication group to use for recreating the contest.
     * @throws IOException When an error occurs communicating with Axon Server.
     * @since 5.0.0
     */
    public static void purgeEventsFromAxonServer(String hostname,
                                                 int port,
                                                 String context,
                                                 boolean dcbContext, String replicationGroup) throws IOException {
        deleteContext(hostname, port, context);
        createContext(hostname, port, context, dcbContext, replicationGroup);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Calls the API of Axon Server at the given {@code hostname} and (http) {@code port} to delete the given
     * {@code context}.
     *
     * @param hostname The hostname where Axon Server can be reached.
     * @param port     The HTTP port Axon Server listens to for API calls.
     * @param context  The context to delete.
     * @throws IOException When an error occurs communicating with Axon Server.
     */
    public static void deleteContext(String hostname, int port, String context) throws IOException {
        URL url = URI.create(String.format("http://%s:%d/v1/context/%s", hostname, port, context)).toURL();
        HttpURLConnection connection = null;
        try {
            connection = openConnection(url);
            connection.setDoOutput(true);
            connection.setRequestMethod("DELETE");
            connection.getInputStream().close();
            int responseCode = connection.getResponseCode();
            Assert.isTrue(202 == responseCode, () -> "The response code [" + responseCode + "] did not match 202.");
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        waitForContextsCondition(hostname, port, contexts -> !contexts.contains(context));
    }

    /**
     * Calls the API of Axon Server at the given {@code hostname} and (http) {@code port} to create a context with the
     * given {@code context} name. The {@code dcbContext} dictates whether the context to be created support DCB, yes or
     * no.
     *
     * @param hostname   The hostname where Axon Server can be reached.
     * @param port       The HTTP port Axon Server listens to for API calls.
     * @param context    The context to create.
     * @param dcbContext A {@code boolean} stating whether a DCB or non-DCB context is being created.
     * @throws IOException When an error occurs communicating with Axon Server.
     */
    public static void createContext(String hostname, int port, String context, boolean dcbContext) throws IOException {
        // TODO this retains the previous behavior but it is flawed, see https://github.com/AxonIQ/axoniq-framework/issues/223
        createContext(hostname, port, context, dcbContext, context);
    }

    /**
     * Calls the API of Axon Server at the given {@code hostname} and (http) {@code port} to create a context with the
     * given {@code context} name. The {@code dcbContext} dictates whether the context to be created support DCB, yes or
     * no.
     *
     * @param hostname         The hostname where Axon Server can be reached.
     * @param port             The HTTP port Axon Server listens to for API calls.
     * @param context          The context to create.
     * @param dcbContext       A {@code boolean} stating whether a DCB or non-DCB context is being created.
     * @param replicationGroup The replication group to be used.
     * @throws IOException When an error occurs communicating with Axon Server.
     */
    public static void createContext(String hostname, int port, String context, boolean dcbContext,
                                     String replicationGroup) throws IOException {
        URL url = URI.create(String.format("http://%s:%d/v1/context", hostname, port)).toURL();
        HttpURLConnection connection = null;
        try {
            String jsonRequest = String.format(
                    "{\"context\": \"%s\", \"dcbContext\": %b, \"replicationGroup\": \"%s\", \"roles\": [{ \"node\": \"axonserver\", \"role\": \"PRIMARY\" }]}",
                    context,
                    dcbContext,
                    replicationGroup
            );
            connection = openConnection(url);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");
            connection.setDoOutput(true);
            connection.setRequestMethod("POST");
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonRequest.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }
            connection.getInputStream().close();
            int responseCode = connection.getResponseCode();
            Assert.isTrue(202 == responseCode, () -> "The response code [" + responseCode + "] did not match 202.");
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        waitForContextsCondition(hostname, port, contexts -> contexts.contains(context));
    }

    /**
     * Opens the given {@code url}'s connection with {@link #HTTP_TIMEOUT_MILLIS} applied as both the connect and
     * read timeout.
     */
    private static HttpURLConnection openConnection(URL url) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(HTTP_TIMEOUT_MILLIS);
        connection.setReadTimeout(HTTP_TIMEOUT_MILLIS);
        return connection;
    }

    private AxonServerContainerUtils() {
        // Utility class
    }
}
