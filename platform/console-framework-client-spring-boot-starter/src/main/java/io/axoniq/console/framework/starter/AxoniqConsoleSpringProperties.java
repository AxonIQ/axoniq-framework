package io.axoniq.console.framework.starter;

import io.axoniq.console.framework.AxoniqConsoleDlqMode;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "axoniq.console")
public class AxoniqConsoleSpringProperties {
    private String host = "framework.console.axoniq.io";
    private Integer port = 7000;
    private String credentials = null;
    private String applicationName = null;
    private AxoniqConsoleDlqMode dlqMode = AxoniqConsoleDlqMode.FULL;
    private boolean secure = true;
    private Long initialDelay = 0L;

    /**
     * The host to connect to. Defaults to {@code framework.console.axoniq.io}.
     */
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    /**
     * The port to connecto. Defaults to {@code 7000}.
     */
    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    /**
     * The credentials used to connect to the AxonIQ Console. The module will not work without setting this.
     */
    public String getCredentials() {
        return credentials;
    }

    public void setCredentials(String credentials) {
        this.credentials = credentials;
    }

    /**
     * The display name of the application in the UI. Defaults to the application name of the Spring Boot application.
     */
    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    /**
     * The mode of DLQ operations. Defaults to {@code FULL}, which can return sensitive information to the UI.
     * If this concerns you, consider {@code MASKED} to mask potentially sensitive data, or {@code NONE} to disable
     * DLQ visibility.
     */
    public AxoniqConsoleDlqMode getDlqMode() {
        return dlqMode;
    }

    public void setDlqMode(AxoniqConsoleDlqMode dlqMode) {
        this.dlqMode = dlqMode;
    }

    /**
     * Whether the connection should use SSL/TLs. Defaults to {@code true}.
     */
    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    /**
     * The initial delay before connecting to the AxonIQ Console in milliseconds. Defaults to {@code 0}.
     */
    public Long getInitialDelay() {
        return initialDelay;
    }

    public void setInitialDelay(Long initialDelay) {
        this.initialDelay = initialDelay;
    }
}
