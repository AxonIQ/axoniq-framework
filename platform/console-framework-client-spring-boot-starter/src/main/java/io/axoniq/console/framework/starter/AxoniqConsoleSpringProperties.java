package io.axoniq.console.framework.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "axoniq.console")
public class AxoniqConsoleSpringProperties {
    private String host = "connector.inspector.axoniq.io";
    private Integer port = 7000;
    private String credentials = null;
    private String applicationName;
    private boolean dlqEnabled = true;
    private boolean secure = true;
    private Long initialDelay = 0L;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getCredentials() {
        return credentials;
    }

    public void setCredentials(String credentials) {
        this.credentials = credentials;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public boolean isDlqEnabled() {
        return dlqEnabled;
    }

    public void setDlqEnabled(boolean dlqEnabled) {
        this.dlqEnabled = dlqEnabled;
    }

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public Long getInitialDelay() {
        return initialDelay;
    }

    public void setInitialDelay(Long initialDelay) {
        this.initialDelay = initialDelay;
    }
}
