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
package io.axoniq.framework.workflow.rig;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minimal TCP forwarder used to partition one node from one backend. A node is pointed at
 * {@link #port()} instead of at the backend directly; {@link #cut()} then drops every live connection and refuses new
 * ones until {@link #heal()}.
 * <p>
 * {@link #connectionsAccepted()} and {@link #connectionsDropped()} exist so a scenario can prove the fault landed
 * rather than assume it.
 *
 * @implNote a thread per direction per connection. Rig traffic is a handful of pooled connections, so a smarter pump
 * would be wasted work.
 */
final class TcpProxy implements AutoCloseable {

    private final ServerSocket listener;
    private final String targetHost;
    private final int targetPort;
    private final Set<Socket> live = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final AtomicInteger accepted = new AtomicInteger();
    private final AtomicInteger dropped = new AtomicInteger();
    private final Thread acceptor;

    TcpProxy(String targetHost, int targetPort) throws IOException {
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.listener = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
        this.acceptor = Thread.ofPlatform().daemon().name("tcp-proxy-" + listener.getLocalPort()).start(this::accept);
    }

    /**
     * Returns the loopback port nodes should connect to.
     *
     * @return the proxy's local port.
     */
    int port() {
        return listener.getLocalPort();
    }

    /**
     * Drops every live connection and refuses new ones. Landing evidence: {@link #connectionsDropped()} increases and
     * the partitioned node starts failing its backend calls.
     */
    void cut() {
        open.set(false);
        for (var socket : Set.copyOf(live)) {
            closeQuietly(socket);
            dropped.incrementAndGet();
        }
        live.clear();
    }

    /**
     * Accepts connections again. Existing pools reconnect on their own.
     */
    void heal() {
        open.set(true);
    }

    /**
     * Returns how many connections the proxy has accepted since it was created.
     *
     * @return accepted connection count.
     */
    int connectionsAccepted() {
        return accepted.get();
    }

    /**
     * Returns how many connections {@link #cut()} has torn down.
     *
     * @return dropped connection count.
     */
    int connectionsDropped() {
        return dropped.get();
    }

    private void accept() {
        while (!listener.isClosed()) {
            Socket downstream = null;
            try {
                downstream = listener.accept();
                if (!open.get()) {
                    closeQuietly(downstream);
                    dropped.incrementAndGet();
                    continue;
                }
                var upstream = new Socket();
                upstream.connect(new InetSocketAddress(targetHost, targetPort), 5_000);
                accepted.incrementAndGet();
                live.add(downstream);
                live.add(upstream);
                pump(downstream, upstream);
                pump(upstream, downstream);
            } catch (IOException e) {
                closeQuietly(downstream);
                if (listener.isClosed()) {
                    return;
                }
            }
        }
    }

    private void pump(Socket from, Socket to) {
        Thread.ofVirtual().start(() -> {
            var buffer = new byte[8192];
            try (from; to) {
                var in = from.getInputStream();
                var out = to.getOutputStream();
                for (int read = in.read(buffer); read >= 0; read = in.read(buffer)) {
                    out.write(buffer, 0, read);
                    out.flush();
                }
            } catch (IOException e) {
                // Either the peer closed or cut() tore the socket down; both are the proxy working as intended.
            } finally {
                live.remove(from);
                live.remove(to);
            }
        });
    }

    private static void closeQuietly(Socket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException ignored) {
            // Nothing useful to do while tearing a socket down.
        }
    }

    @Override
    public void close() {
        cut();
        try {
            listener.close();
        } catch (IOException ignored) {
            // Nothing useful to do while shutting the proxy down.
        }
        acceptor.interrupt();
    }
}
