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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.framework.extension.dataprotection.cryptoengine;

import io.axoniq.framework.extension.dataprotection.internal.utils.ExceptionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Base64;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.sql.DataSource;

/**
 * JDBC-based implementation of the {@link CryptoEngine} interface, included for users who wish to store keys
 * in a relational database but do not wish to use JPA.
 *
 * @author Frans van Buul
 */
public class JdbcCryptoEngine extends DatabaseBackedCryptoEngine {

    private final DataSource dataSource;
    private final String tableName;
    private final String keyIdColumnName;
    private final String keyDataColumnName;

    /**
     * Constructs a new JdbcCryptoEngine.
     *
     * @param dataSource        the DataSource
     * @param tableName         the SQL name table in which keys will be stored
     * @param keyIdColumnName   the SQL name of the column in which the key id will be stored
     * @param keyDataColumnName the SQL name of the column in which the key data will be stored
     */
    public JdbcCryptoEngine(DataSource dataSource, String tableName, String keyIdColumnName, String keyDataColumnName) {
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.keyIdColumnName = keyIdColumnName;
        this.keyDataColumnName = keyDataColumnName;
    }

    /**
     * Constructs a new JdbcCryptoEngine, using "id" as the SQL name of the key id column, and "secret_key" as the SQL
     * name of the key data column.
     *
     * @param dataSource the DataSource
     * @param tableName  the SQL name table in which keys will be stored
     */
    public JdbcCryptoEngine(DataSource dataSource, String tableName) {
        this(dataSource, tableName, "id", "secret_key");
    }

    /**
     * Constructs a new JdbcCryptoEngine, using "id" as the SQL name of the key id column, "secret_key" as the SQL name
     * of the key data column, and "data_protection_keys" as the SQL name of the table.
     *
     * @param dataSource the DataSource
     */
    public JdbcCryptoEngine(DataSource dataSource) {
        this(dataSource, "data_protection_keys");
    }

    /**
     * Returns the SQL name of the table as configured during construction.
     *
     * @return the name
     */
    protected String getTableName() {
        return tableName;
    }

    /**
     * Returns the SQL name of the key id column as configured during construction.
     *
     * @return the name
     */
    protected String getKeyIdColumnName() {
        return keyIdColumnName;
    }

    /**
     * Returns the SQL name of the key data column as configured during construction.
     *
     * @return the name
     */
    protected String getKeyDataColumnName() {
        return keyDataColumnName;
    }

    /**
     * Returns a DDL statement to create the table for key storage, using the SQL names that have been
     * configured. This is never executed automatically by the module. It is purely here to facilitate
     * developer who need to create the table automatically, e.g. for automatic tests.
     * <p>
     * This statement uses {@code VARCHAR(255)} as the data type for the columns, since this will work
     * universally across SQL databases. Please note that for MS SQL Server, you may wish to either
     * use {@code sendStringParametersAsUnicode=false}, or change this to {@code NVARCHAR}, to avoid
     * a performance hit on the primary key index.
     *
     * @return the CREATE TABLE statement
     *
     * @see <a href="https://blogs.msdn.microsoft.com/sqlcat/2010/04/05/character-data-type-conversion-when-using-sql-server-jdbc-drivers/">SQL Server Customer Advisory Team</a>
     */
    public String getCreateTableStatement() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(getTableName()).append(" ( ");
        sb.append(getKeyIdColumnName()).append(" VARCHAR(255) NOT NULL, ");
        sb.append(getKeyDataColumnName()).append(" VARCHAR(255) NOT NULL, ");
        sb.append("PRIMARY KEY (").append(getKeyIdColumnName()).append(") );");
        return sb.toString();
    }

    /**
     * Generates the SELECT statement to retrieve the key data based on the key id:
     * <p>
     * {@code SELECT [key data column] FROM [key table] WHERE [key id column] = ?}
     *
     * @return the SELECT statement
     */
    protected String getSelectStatement() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(getKeyDataColumnName());
        sb.append(" FROM ").append(getTableName());
        sb.append(" WHERE ").append(getKeyIdColumnName()).append(" = ?");
        return sb.toString();
    }

    /**
     * Generates the INSERT statement to store a key:
     * <p>
     * {@code INSERT INTO [key table]([key id column], [key data column]) VALUES (?, ?)}
     *
     * @return the INSERT statement
     */
    protected String getInsertStatement() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(getTableName());
        sb.append("(").append(getKeyIdColumnName()).append(", ").append(getKeyDataColumnName()).append(")");
        sb.append(" VALUES (?, ?)");
        return sb.toString();
    }

    /**
     * Generates the DELETE statement to remove a key:
     * <p>
     * {@code DELETE FROM [key table] WHERE [key id column] = ?}
     *
     * @return the DELETE statement
     */
    protected String getDeleteStatement() {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(getTableName());
        sb.append(" WHERE ").append(getKeyIdColumnName()).append(" = ?");
        return sb.toString();
    }

    @Override
    protected SecretKey putKeyIfAbsent(String id, SecretKeySpec secretKeySpec) {
        SecretKey secretKey = getKey(id);
        while(secretKey == null) {
            try(Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(true);
                try(PreparedStatement statement = connection.prepareStatement(getInsertStatement())) {
                    statement.setString(1, id);
                    statement.setString(2, Base64.getEncoder().encodeToString(secretKeySpec.getEncoded()));
                    statement.executeUpdate();
                }
                secretKey = secretKeySpec;
            } catch(SQLIntegrityConstraintViolationException ex) {
                secretKey = getKey(id);
            } catch(SQLException ex) {
                throw ExceptionFactory.forSQLException(ex);
            }
        }
        return secretKey;
    }

    @Override
    public SecretKey getKey(String id) {
        try(Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            try(PreparedStatement statement = connection.prepareStatement(getSelectStatement())) {
                statement.setString(1, id);
                try(ResultSet resultSet = statement.executeQuery()) {
                    if(resultSet.next()) {
                        return new SecretKeySpec(Base64.getDecoder().decode(resultSet.getString(1)), "AES");
                    } else {
                        return null;
                    }
                }
            }
        } catch(SQLException ex) {
            throw ExceptionFactory.forSQLException(ex);
        }
    }

    @Override
    public void deleteKey(String id) {
        try(Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(true);
            try(PreparedStatement statement = connection.prepareStatement(getDeleteStatement())) {
                statement.setString(1, id);
                statement.executeUpdate();
            }
        } catch(SQLException ex) {
            throw ExceptionFactory.forSQLException(ex);
        }
    }
}
