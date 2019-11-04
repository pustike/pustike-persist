/*
 * Copyright (C) 2016-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.pustike.persist.sql;

import java.sql.SQLException;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.sql.DataSource;

import io.github.pustike.persist.metadata.Schema;

/**
 * The transaction manager with dataSource and schema. Multiple calls to {@link #execute(Function)}
 * or {@link #transact(Consumer)} methods within the same thread will get the same instance of sqlQuery and transaction.
 */
public final class Repository {
    private final DataSource dataSource;
    private final Schema schema;
    private final ThreadLocal<SqlQuery> threadCtx;

    /**
     * Create an instance of repository with the given dataSource and entity schema definition.
     * @param dataSource the data source
     * @param schema the entity schema metadata
     */
    public Repository(DataSource dataSource, Schema schema) {
        this.dataSource = dataSource;
        this.schema = schema;
        this.threadCtx = new ThreadLocal<>();
    }

    /**
     * Get the data source of this repository.
     * @return the data source
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Get the configured entity schema metadata instance.
     * @return the schema metadata
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Executes the given function in a transaction. If a transaction is already present in the current thread,
     * joins the same transaction else creates a new transaction.
     * @param function the user function to execute
     * @param <R> the return data type
     * @return the return value from the function
     */
    public <R> R execute(Function<SqlQuery, R> function) {
        return executeInTransaction(function, null);
    }

    /**
     * Execute the given consumer function in a transaction. If a transaction is already present in the current thread,
     * joins the same transaction else creates a new transaction.
     * @param consumer the consumer function to execute
     */
    public void transact(Consumer<SqlQuery> consumer) {
        executeInTransaction(null, consumer);
    }

    private <R> R executeInTransaction(Function<SqlQuery, R> function, Consumer<SqlQuery> consumer) {
        SqlQuery sqlQuery = getSqlQuery();
        boolean onSuccess = true;
        try {
            sqlQuery.incrementCounter();
            if (function != null) {
                return function.apply(sqlQuery);
            } else if (consumer != null) {
                consumer.accept(sqlQuery);
            }
        } catch (Throwable th) {
            onSuccess = false;
            throw th;
        } finally {
            if (sqlQuery.decrementCounter() == 0) {
                threadCtx.remove();
                sqlQuery.close(onSuccess);
            }
        }
        return null;
    }

    private SqlQuery getSqlQuery() {
        SqlQuery sqlQuery = threadCtx.get();
        if (sqlQuery == null) {
            try {
                sqlQuery = new SqlQuery(dataSource.getConnection(), schema);
                sqlQuery.beginTransaction();
            } catch (SQLException ex) {
                throw new RuntimeException("Couldn't establish database connection", ex);
            }
            threadCtx.set(sqlQuery);
        }
        return sqlQuery;
    }
}
