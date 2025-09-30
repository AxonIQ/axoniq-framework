package io.axoniq.framework.postgresql;

import org.axonframework.common.annotations.Internal;
import org.axonframework.messaging.unitofwork.ProcessingContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

/**
 * An implementation of {@link ConnectionExecutor} which uses a {@link DataSource}
 * to obtain JDBC connections.
 *
 * @author John Hendrikx
 * @since 0.1.0
 */
@Internal
public class DataSourceConnectionExecutor implements ConnectionExecutor {
    private final DataSource dataSource;

    /**
     * Constructs a new instance using the given data source.
     *
     * @param dataSource a data source, cannot be {@code null}
     * @throws NullPointerException when any argument is {@code null}
     */
    public DataSourceConnectionExecutor(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public <R> R execute(ProcessingContext context, JdbcFunction<Connection, R> function) {
        try (Connection connection = dataSource.getConnection()) {
            R result = function.apply(connection);

            connection.commit();

            return result;
        }
        catch (SQLException e) {
            throw new IllegalStateException("SQL callback failed with an SQLException", e);
        }
    }
}
