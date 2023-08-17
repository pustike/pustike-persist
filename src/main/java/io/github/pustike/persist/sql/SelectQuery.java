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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.github.pustike.persist.metadata.ColumnType;
import io.github.pustike.persist.metadata.EntityData;
import io.github.pustike.persist.metadata.FieldData;
import io.github.pustike.persist.metadata.Schema;

final class SelectQuery {
    private static final Logger logger = System.getLogger(SelectQuery.class.getName());
    private final SqlQuery sqlQuery;
    private final Class<?> entityClass;
    private final String fieldGroup;
    private final Object primaryKey;
    private final String alias;

    SelectQuery(SqlQuery sqlQuery, Class<?> entityClass, String fieldGroup, Object primaryKey) {
        this.sqlQuery = sqlQuery;
        this.entityClass = entityClass;
        this.fieldGroup = fieldGroup;
        this.primaryKey = Objects.requireNonNull(primaryKey);
        this.alias = "x";
    }

    <E> E find(boolean forUpdate) {
        Map<String, List<String>> aliasFieldNamesMap = new HashMap<>();
        String queryString = buildQuery(aliasFieldNamesMap);
        if (forUpdate) {
            queryString = queryString + " for update of " + alias;
        }
        logger.log(Level.DEBUG, queryString);
        try (PreparedStatement stmt = sqlQuery.getConnection().prepareStatement(queryString)) {
            stmt.setObject(1, primaryKey);
            try (ResultSet resultSet = stmt.executeQuery()) {
                return resultSet.next() ? toResultData(resultSet, aliasFieldNamesMap) : null;
            }
        } catch (Exception e) {
            throw new RuntimeException("Couldn't execute query", e);
        }
    }

    private String buildQuery(Map<String, List<String>> aliasFieldNamesMap) {
        Schema schema = sqlQuery.getSchema();
        EntityData entityData = schema.getEntityData(entityClass);
        Set<String> fieldGroupFields = entityData.getFieldGroupFields(fieldGroup);
        int fkCounter = 0;
        List<String> selectedFieldNames = new ArrayList<>();
        StringBuilder queryBuilder = new StringBuilder("select");
        StringBuilder joinBuilder = new StringBuilder();
        for (String fieldName : fieldGroupFields) {
            int fgIndex = fieldName.indexOf('@');
            String joinFieldGroup = null;
            if (fgIndex != -1) {
                joinFieldGroup = fieldName.substring(fgIndex + 1);
                fieldName = fieldName.substring(0, fgIndex);
            }
            FieldData fieldData = entityData.getFieldData(fieldName);
            if (fieldData.getColumnType() == ColumnType.ForeignKey) {
                EntityData fkEntityData = schema.getEntityData(fieldData.getFieldType());
                String asAlias = "t" + (fkCounter++);
                joinBuilder.append(getJoinClause(fkEntityData, fieldData, asAlias));
                List<String> selectedFkFieldNames = new ArrayList<>();
                for (String fkFieldName : fkEntityData.getJoinFetchFields(joinFieldGroup)) {
                    int fgIndex2 = fkFieldName.indexOf('@');
                    fkFieldName = fgIndex2 == -1 ? fkFieldName : fkFieldName.substring(0, fgIndex2);
                    FieldData fkFieldData = fkEntityData.getFieldData(fkFieldName);
                    selectedFkFieldNames.add(fkFieldData.getName());
                    queryBuilder.append(' ').append(asAlias).append('.')
                        .append(fkFieldData.getColumnName()).append(',');
                }
                selectedFieldNames.add(fieldData.getName() + '@' + asAlias);
                aliasFieldNamesMap.put(fieldData.getFieldType().getSimpleName() + '@' + asAlias, selectedFkFieldNames);
            } else {
                selectedFieldNames.add(fieldData.getName());
                queryBuilder.append(' ').append(alias).append('.').append(fieldData.getColumnName()).append(',');
            }
        }
        queryBuilder.setLength(queryBuilder.length() - 1);
        aliasFieldNamesMap.put(entityClass.getSimpleName() + '@' + alias, selectedFieldNames);
        //
        queryBuilder.append(" FROM ").append(schema.toSchemaTableName(entityData)).append(" as ").append(alias);
        queryBuilder.append(joinBuilder);
        //
        FieldData idField = entityData.getIdField();
        queryBuilder.append(" WHERE ").append(alias).append('.').append(idField.getColumnName()).append(" = ?");
        return queryBuilder.toString();
    }

    @SuppressWarnings("unchecked")
    private <T> T toResultData(ResultSet resultSet, Map<String, List<String>> aliasFieldNamesMap) throws Exception {
        int index = 1;
        T instance = (T) entityClass.getDeclaredConstructor().newInstance();
        Schema schema = sqlQuery.getSchema();
        EntityData entityData = schema.getEntityData(entityClass);
        List<String> fieldAliasList = aliasFieldNamesMap.get(entityClass.getSimpleName() + '@' + alias);
        for (String fieldAlias : fieldAliasList) {
            String[] fieldAliasSplit = fieldAlias.split("@");
            FieldData fieldData = entityData.getFieldData(fieldAliasSplit[0]);
            if (fieldData.getColumnType() == ColumnType.ForeignKey) {
                Class<?> fkFieldType = fieldData.getFieldType();
                String key = fkFieldType.getSimpleName() + '@' + fieldAliasSplit[1];
                List<String> joinFetchFields = aliasFieldNamesMap.get(key);
                EntityData fkEntityData = schema.getEntityData(fkFieldType);
                Object fkInstance = null;// TODO cache these instances based on id to avoid multiple object creations
                for (String fieldName : joinFetchFields) {
                    FieldData fkFieldData = fkEntityData.getFieldData(fieldName);
                    if (fkFieldData.getColumnType() != ColumnType.ForeignKey) {
                        Class<?> resultType = fkFieldData.getResultType();
                        Object value = resultType == null ? resultSet.getObject(index++)
                            : resultSet.getObject(index++, resultType);
                        if (value != null && fkInstance == null) {
                            fkInstance = fkEntityData.createInstance();
                        }
                        if (fkInstance != null) {
                            fkFieldData.setValue(fkInstance, value);
                        }
                    } else {// only load the id from the 2nd level foreign key!
                        Class<?> fkFieldType2 = fkFieldData.getFieldType();
                        EntityData fkEntityData2 = schema.getEntityData(fkFieldType2);
                        FieldData fkIdField2 = fkEntityData2.getIdField();
                        Class<?> resultType = fkIdField2.getResultType();
                        Object value = resultType == null ? resultSet.getObject(index++)
                            : resultSet.getObject(index++, resultType);
                        if (value != null) {
                            Object fkInstance2 = fkEntityData2.createInstance();
                            fkIdField2.setValue(fkInstance2, value);
                            fkFieldData.setValue(fkInstance, fkInstance2);
                        }
                    }
                }
                fieldData.setValue(instance, fkInstance);
            } else {
                Class<?> resultType = fieldData.getResultType();
                fieldData.setValue(instance, resultType == null ? resultSet.getObject(index++)
                    : resultSet.getObject(index++, resultType));
            }
        }
        return instance;
    }

    private String getJoinClause(EntityData fkData, FieldData fieldData, String asAlias) {
        String joinType = fieldData.isOptional() ? " left outer join " : " inner join ";
        StringBuilder queryBuilder = new StringBuilder(joinType).append(sqlQuery.getSchema().toSchemaTableName(fkData))
            .append(" as ").append(asAlias).append(" on ")
            .append(asAlias).append('.').append(fkData.getIdField().getColumnName()).append(" = ");
        return queryBuilder.append(alias).append('.').append(fieldData.getColumnName()).toString();
    }
}
