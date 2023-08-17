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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import io.github.pustike.persist.EntityListener;
import io.github.pustike.persist.metadata.ColumnType;
import io.github.pustike.persist.metadata.EntityData;
import io.github.pustike.persist.metadata.FieldData;
import io.github.pustike.persist.metadata.Schema;

final class EntitySql {
    private static final Logger logger = System.getLogger(EntitySql.class.getName());
    private static final int INSERT = 0, UPDATE = 1;
    private final SqlQuery sqlQuery;
    private final String queryString;
    private final List<String> parameterFields;
    private final int sqlType;
    private Map<String, FieldData> generatedValues;

    private EntitySql(SqlQuery sqlQuery, String queryString, List<String> parameterFields, int sqlType) {
        this.sqlQuery = sqlQuery;
        this.queryString = queryString;
        this.parameterFields = parameterFields;
        this.generatedValues = new LinkedHashMap<>();
        this.sqlType = sqlType;
    }

    static EntitySql insert(SqlQuery sqlQuery, Class<?> entityClass) {
        return insert(sqlQuery, entityClass, null, null);
    }

    static EntitySql insert(SqlQuery sqlQuery, Class<?> entityClass, String onConflict, String updateClause) {
        Schema schema = sqlQuery.getSchema();
        EntityData entityData = schema.getEntityData(entityClass);
        StringBuilder queryBuilder = new StringBuilder("insert into ")
            .append(schema.toSchemaTableName(entityData)).append(" as x (");
        int parameterCount = 0;
        List<String> parameterFields = new ArrayList<>();
        for (FieldData fieldData : entityData.getFieldData()) {
            if (fieldData.getColumnType() == ColumnType.Id) {
                continue;// it is auto-generated // review: for composite keys
            }
            parameterFields.add(fieldData.getName());
            queryBuilder.append(fieldData.getColumnName()).append(',');
            parameterCount++;
        }
        queryBuilder.setLength(queryBuilder.length() - 1);// to remove the last comma(,)
        queryBuilder.append(") values (").append("?,".repeat(parameterCount));
        queryBuilder.setLength(queryBuilder.length() - 1);// to remove the last comma(,)
        queryBuilder.append(')');
        if (onConflict != null) {
            queryBuilder.append(" ON CONFLICT (");
            String[] fieldNames = onConflict.split(",");
            List<String> columnNames = new ArrayList<>();
            for (String fieldName : fieldNames) {
                columnNames.add(entityData.getFieldData(fieldName.trim()).getColumnName());
            }
            queryBuilder.append(String.join(", ", columnNames)).append(")");
            if (updateClause == null) {
                queryBuilder.append(" DO NOTHING");
            } else {
                queryBuilder.append(" DO UPDATE SET ").append(updateClause);
            }
        }
        EntitySql entitySql = new EntitySql(sqlQuery, queryBuilder.toString(), parameterFields, INSERT);
        entitySql.putGeneratedField(entityData.getIdField());
        FieldData versionField = entityData.getVersionField();
        if (versionField != null) {
            entitySql.putGeneratedField(versionField);
        }
        return entitySql;
    }

    private void putGeneratedField(FieldData fieldData) {
        generatedValues.put(fieldData.getColumnName(), fieldData);
    }

    static EntitySql update(SqlQuery sqlQuery, Class<?> entityClass, String fieldGroup) {
        Schema schema = sqlQuery.getSchema();
        EntityData entityData = schema.getEntityData(entityClass);
        StringBuilder queryBuilder = new StringBuilder("update ")
            .append(schema.toSchemaTableName(entityData)).append(" set ");
        FieldData versionField = entityData.getVersionField();
        if (versionField != null) {
            String columnName = versionField.getColumnName();
            queryBuilder.append(columnName).append(" = ").append(columnName).append(" + 1,");
        }
        List<String> parameterFields = new ArrayList<>();
        Set<String> fieldGroupFields = entityData.getFieldGroupFields(fieldGroup);
        for (String fieldName : fieldGroupFields) {// exclude Id/Version fields
            int fgIndex = fieldName.indexOf('@');
            fieldName = fgIndex == -1 ? fieldName : fieldName.substring(0, fgIndex);
            FieldData fieldData = entityData.getFieldData(fieldName);
            if (fieldData.getColumnType() == ColumnType.Id || fieldData.getColumnType() == ColumnType.Version) {
                continue;// it is auto-generated // review: for composite keys
            }
            parameterFields.add(fieldName);
            queryBuilder.append(' ').append(fieldData.getColumnName()).append(" = ?,");
        }
        if (parameterFields.isEmpty()) {
            logger.log(Level.WARNING, "Fields are not included to update!");
            return null;// fields are not included to update!
        }
        queryBuilder.setLength(queryBuilder.length() - 1);// to remove the last comma(,)
        FieldData idField = entityData.getIdField();
        parameterFields.add(idField.getName());
        queryBuilder.append(" where ").append(idField.getColumnName()).append(" = ?");
        if (versionField != null) {
            parameterFields.add(versionField.getName());
            queryBuilder.append(" and ").append(versionField.getColumnName()).append(" = ?");
        }
        EntitySql updateQuery = new EntitySql(sqlQuery, queryBuilder.toString(), parameterFields, UPDATE);
        if (versionField != null) {
            updateQuery.putGeneratedField(versionField);
        }
        return updateQuery;
    }

    static int delete(SqlQuery sqlQuery, Object entity) {
        Schema schema = sqlQuery.getSchema();
        EntityData entityData = schema.getEntityData(entity.getClass());
        StringBuilder queryBuilder = new StringBuilder("DELETE FROM ")
            .append(schema.toSchemaTableName(entityData)).append(" WHERE ");
        FieldData idField = entityData.getIdField();
        queryBuilder.append(idField.getColumnName()).append(" = ?");
        FieldData versionField = entityData.getVersionField();
        if (versionField != null) {
            queryBuilder.append(" AND ").append(versionField.getColumnName()).append(" = ?");
        }
        logger.log(Level.DEBUG, queryBuilder.toString());
        try (PreparedStatement stmt = sqlQuery.getConnection().prepareStatement(queryBuilder.toString())) {
            stmt.setObject(1, idField.getValue(entity), idField.getJdbcType());
            if (versionField != null) {
                stmt.setObject(2, versionField.getValue(entity), versionField.getJdbcType());
            }
            return stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't execute query", e);
        }
    }

    static <E> void batchDelete(SqlQuery sqlQuery, List<E> entityList) {
        if (entityList.isEmpty()) {
            return;
        }
        Schema schema = sqlQuery.getSchema();
        Class<?> entityClass = entityList.get(0).getClass();
        EntityData entityData = schema.getEntityData(entityClass);
        StringBuilder queryBuilder = new StringBuilder("DELETE FROM ")
            .append(schema.toSchemaTableName(entityData)).append(" WHERE ");
        FieldData idField = entityData.getIdField();
        queryBuilder.append(idField.getColumnName()).append(" = ?");
        FieldData versionField = entityData.getVersionField();
        if (versionField != null) {
            queryBuilder.append(" AND ").append(versionField.getColumnName()).append(" = ?");
        }
        logger.log(Level.DEBUG, queryBuilder.toString());
        final int batchSize = 100, totalCount = entityList.size();
        try (PreparedStatement stmt = sqlQuery.getConnection().prepareStatement(queryBuilder.toString())) {
            for (int idx = 0; idx < totalCount; idx++) {
                E entity = entityList.get(idx);
                stmt.setObject(1, idField.getValue(entity), idField.getJdbcType());
                if (versionField != null) {
                    stmt.setObject(2, versionField.getValue(entity), versionField.getJdbcType());
                }
                stmt.addBatch();
                if ((idx + 1) % batchSize == 0 || idx == totalCount - 1) {
                    stmt.executeBatch();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't execute query", e);
        }
    }

    static void lockForUpdate(SqlQuery sqlQuery, Class<?> entityClass, Object primaryKey) {
        Schema schema = sqlQuery.getSchema();
        EntityData entityData = schema.getEntityData(entityClass);
        StringBuilder queryBuilder = new StringBuilder("SELECT 1 FROM ")
            .append(schema.toSchemaTableName(entityData)).append(" WHERE ");
        FieldData idField = entityData.getIdField();
        queryBuilder.append(idField.getColumnName()).append(" = ? FOR UPDATE");
        logger.log(Level.DEBUG, queryBuilder.toString());
        try (PreparedStatement stmt = sqlQuery.getConnection().prepareStatement(queryBuilder.toString())) {
            stmt.setObject(1, primaryKey, idField.getJdbcType());
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't execute query", e);
        }
    }

    void execute(Object instance) {
        String[] columnNames = getGeneratedColumns();
        logger.log(Level.DEBUG, queryString);
        try (PreparedStatement stmt = sqlQuery.getConnection().prepareStatement(queryString, columnNames)) {
            List<Entry<Integer, Object>> parameterList = getParameters(instance);
            for (int i = 0; i < parameterList.size(); i++) {
                Entry<Integer, Object> param = parameterList.get(i);
                stmt.setObject(i + 1, param.getValue(), param.getKey());
            }
            int updateCount = stmt.executeUpdate();
            if (updateCount <= 0) {
                return;// TODO should it throw exception here on update?
            }
            try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                while (generatedKeys.next()) {
                    for (String columnName : columnNames) {
                        Object object = generatedKeys.getObject(columnName);
                        setGeneratedValue(instance, columnName, object);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't execute query", e);
        }
    }

    private String[] getGeneratedColumns() {
        return generatedValues.keySet().toArray(new String[0]);
    }

    private List<Entry<Integer, Object>> getParameters(Object instance) {
        Schema schema = sqlQuery.getSchema();
        notifyEntityListener(schema, instance);
        List<Entry<Integer, Object>> parameterList = new ArrayList<>();
        EntityData entityData = schema.getEntityData(instance.getClass());
        for (String fieldName : parameterFields) {
            FieldData fieldData = entityData.getFieldData(fieldName);
            if (sqlType == INSERT && fieldData.getColumnType() == ColumnType.Version) {// set the value manually
                parameterList.add(new SimpleImmutableEntry<>(fieldData.getJdbcType(), 1));
            } else if (fieldData.getColumnType() == ColumnType.ForeignKey) {// set the value manually
                Object value = fieldData.getValue(instance);
                if (value != null) {
                    Class<?> entityType = fieldData.getFieldType();
                    EntityData fkEntityData = schema.getEntityData(entityType);
                    value = fkEntityData.getIdField().getValue(value);
                }
                parameterList.add(new SimpleImmutableEntry<>(fieldData.getJdbcType(), value));
            } else {
                parameterList.add(new SimpleImmutableEntry<>(fieldData.getJdbcType(), fieldData.getValue(instance)));
            }
        }
        return parameterList;
    }

    private void setGeneratedValue(Object entity, String columnName, Object value) {
        FieldData fieldData = generatedValues.get(columnName);
        fieldData.setValue(entity, value);
    }

    private void notifyEntityListener(Schema schema, Object instance) {
        EntityListener entityListener = schema.getEntityListener();
        if (entityListener != null) {
            if (sqlType == INSERT) {
                entityListener.beforeInsert(instance);
            } else if (sqlType == UPDATE) {
                entityListener.beforeUpdate(instance);
            }
        }
    }

    <E> void executeInBatch(List<E> instanceList) {
        logger.log(Level.DEBUG, queryString);
        final int batchSize = 100, totalCount = instanceList.size();
        String[] columnNames = getGeneratedColumns();
        try (PreparedStatement stmt = sqlQuery.getConnection().prepareStatement(queryString, columnNames)) {
            for (int idx = 0; idx < totalCount; idx++) {
                E entity = instanceList.get(idx);
                List<Entry<Integer, Object>> parameterList = getParameters(entity);
                for (int i = 0; i < parameterList.size(); i++) {
                    Entry<Integer, Object> param = parameterList.get(i);
                    stmt.setObject(i + 1, param.getValue(), param.getKey());
                }
                stmt.addBatch();
                if ((idx + 1) % batchSize == 0 || idx == totalCount - 1) {
                    stmt.executeBatch();
                    try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
                        int idx2 = idx - (idx % batchSize);
                        while (generatedKeys.next()) {
                            entity = instanceList.get(idx2++);
                            for (String columnName : columnNames) {
                                Object object = generatedKeys.getObject(columnName);
                                setGeneratedValue(entity, columnName, object);
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't execute query", e);
        }
    }
}
