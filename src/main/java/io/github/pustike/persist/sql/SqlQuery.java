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

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import io.github.pustike.persist.metadata.Schema;

/**
 * The query object, created by the repository, which can be used to insert, update, delete, select, find data.
 */
public final class SqlQuery {
    private static final Logger logger = System.getLogger(SqlQuery.class.getName());
    private final Connection connection;
    private final Schema schema;
    private int counter = 0;

    SqlQuery(Connection connection, Schema schema) {
        this.connection = connection;
        this.schema = schema;
    }

    /**
     * Get the connection instance created for this transaction.
     * @return the database connection
     */
    Connection getConnection() {
        return connection;
    }

    Schema getSchema() {
        return schema;
    }

    void incrementCounter() {
        counter++;
    }

    int decrementCounter() {
        return --counter;
    }

    void beginTransaction() {
        try {
            connection.setAutoCommit(false);// begin transaction
        } catch (Exception ex) {
            throw new RuntimeException("Couldn't begin transaction", ex);
        }
    }

    void close(boolean onSuccess) {
        try {
            if (onSuccess) {// commit changes
                connection.commit();
            } else { // rollback uncommitted changes
                connection.rollback();
            }
        } catch (Exception ex) {
            try {// ensure rollback is called if commit fails!
                connection.rollback();
            } catch (Exception ignored) {
            }
            throw new RuntimeException("Couldn't commit transaction", ex);
        } finally {
            try {// close connection
                connection.close();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Execute a statement to insert the given entity data.
     * @param entity a new entity instance to be inserted
     */
    public void insert(Object entity) {
        Objects.requireNonNull(entity);
        EntitySql.insert(this, entity.getClass()).execute(entity);
    }

    /**
     * Execute a statement to insert the given list of entities in a batch size of 100.
     * @param entityList a list of new entities to be inserted
     * @param <E> type of the entity
     */
    public <E> void batchInsert(List<E> entityList) {
        if (entityList.isEmpty()) {
            return;
        }
        Class<?> entityClass = entityList.get(0).getClass();
        EntitySql.insert(this, entityClass).executeInBatch(entityList);
    }

    /**
     * Execute a statement to insert the given list of entities in batch of 100 and on conflict update the data.
     * @param entityList a list of entities to be inserted or updated
     * @param onConflict columns defining the conflict constraint
     * @param updateClause the update clause for the excluded data as a native query
     * @param <E> type of the entity
     */
    public <E> void batchUpsert(List<E> entityList, String onConflict, String updateClause) {
        if (entityList.isEmpty()) {
            return;
        }
        Class<?> entityClass = entityList.get(0).getClass();
        EntitySql.insert(this, entityClass, onConflict, updateClause).executeInBatch(entityList);
    }

    /**
     * Execute a statement to update specified fields on the given entity data.
     * @param entity an existing entity instance to be updated
     * @param fieldGroup the name of the field group
     */
    public void update(Object entity, String fieldGroup) {
        Objects.requireNonNull(entity);
        EntitySql entitySql = EntitySql.update(this, entity.getClass(), fieldGroup);
        if (entitySql != null) {
            entitySql.execute(entity);
        }
    }

    /**
     * Execute a statement to update specified fields on the given list of entities.
     * @param entityList a list of existing entities to be updated
     * @param fieldGroup the name of the field group
     * @param <E> type of the entity
     */
    public <E> void batchUpdate(List<E> entityList, String fieldGroup) {
        if (entityList.isEmpty()) {
            return;
        }
        Class<?> entityClass = entityList.get(0).getClass();
        EntitySql entitySql = EntitySql.update(this, entityClass, fieldGroup);
        if (entitySql != null) {
            entitySql.executeInBatch(entityList);
        }
    }

    /**
     * Insert or update the given entity.
     * @param entity the entity instance to be saved.
     * @param isNewEntity execute insert query if new entity, else update with the given field group
     * @param fieldGroup the name of the field group
     * @param <E> type of the entity
     * @return the same instance of the entity set with returned values, like id, version
     */
    public <E> E save(E entity, boolean isNewEntity, String fieldGroup) {
        if (isNewEntity) {
            insert(entity);
        } else {
            update(entity, fieldGroup);
        }
        return entity;
    }

    /**
     * Execute a statement to delete the given entity.
     * @param entity the entity to be deleted
     * @return the modified row count
     */
    public int delete(Object entity) {
        Objects.requireNonNull(entity);
        return EntitySql.delete(this, entity);
    }

    /**
     * Execute a statement to delete the given list of entities.
     * @param entityList the list of entities to be deleted
     * @param <T> type of the entity
     */
    public <T> void batchDelete(List<T> entityList) {
        if (entityList.isEmpty()) {
            return;
        }
        EntitySql.batchDelete(this, entityList);
    }

    /**
     * Execute a select from entity table, on the given primary key, for update.
     * @param entityClass the entity class
     * @param primaryKey the primary key value to select
     * @param <E> type of the entity
     */
    public <E> void lockForUpdate(Class<E> entityClass, Object primaryKey) {
        EntitySql.lockForUpdate(this, entityClass, primaryKey);
    }

    /**
     * Execute a select from entity table, on the given primary key and return the entity instance with all values set.
     * @param entityClass the entity class
     * @param primaryKey the primary key value to select
     * @param <E> type of the entity
     * @return an entity instance with all selected values set
     */
    public <E> E select(Class<E> entityClass, Object primaryKey) {
        return select(entityClass, primaryKey, null);
    }

    /**
     * Execute a select columns from fieldGroup, from entity table, on the given primary key
     * and return the entity instance with all values set.
     * @param entityClass the entity class
     * @param primaryKey the primary key value to select
     * @param fieldGroup name of the field group
     * @param <E> type of the entity
     * @return an entity instance with all selected values set
     */
    public <E> E select(Class<E> entityClass, Object primaryKey, String fieldGroup) {
        return new SelectQuery(this, entityClass, fieldGroup, primaryKey).find(false);
    }

    /**
     * Execute a select from entity table, on the given primary key, for update
     * and return the entity instance with all values set.
     * @param entityClass the entity class
     * @param primaryKey the primary key value to select
     * @param <E> type of the entity
     * @return an entity instance with all selected values set
     */
    public <E> E selectForUpdate(Class<E> entityClass, Object primaryKey) {
        return selectForUpdate(entityClass, primaryKey, null);
    }

    /**
     * Execute a select columns from fieldGroup, from entity table, on the given primary key, for update
     * and return the entity instance with all values set.
     * @param entityClass the entity class
     * @param primaryKey the primary key value to select
     * @param fieldGroup name of the field group
     * @param <E> type of the entity
     * @return an entity instance with all selected values set
     */
    public <E> E selectForUpdate(Class<E> entityClass, Object primaryKey, String fieldGroup) {
        return new SelectQuery(this, entityClass, fieldGroup, primaryKey).find(true);
    }

    /**
     * Create a new {@link Finder} instance for the given entity class.
     * @param entityClass the entity class
     * @param <E> type of the entity
     * @return the finder api for the given entity
     */
    public <E> Finder<E> find(Class<E> entityClass) {
        return find(entityClass, "x");
    }

    /**
     * Create a new {@link Finder} instance for the given entity class using the sql alias.
     * @param entityClass the entity class
     * @param alias the sql alias to be used for the entity table
     * @param <E> type of the entity
     * @return the finder api for the given entity
     */
    public <E> Finder<E> find(Class<E> entityClass, String alias) {
        return new Finder<>(this, entityClass, alias);
    }

    /**
     * Execute a prepared statement with the given queryString and parameters.
     * @param queryString the sql query to execute
     * @param parameters an array of parameters in the order
     * @return the updated row count
     */
    public int executeUpdate(String queryString, Object... parameters) {
        logger.log(Level.INFO, queryString);
        try (PreparedStatement stmt = connection.prepareStatement(queryString)) {
            for (int i = 0; i < parameters.length; i++) {
                stmt.setObject(i + 1, parameters[i]);
            }
            return stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't execute query", e);
        }
    }
}
