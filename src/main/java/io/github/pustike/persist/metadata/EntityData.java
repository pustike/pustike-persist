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
package io.github.pustike.persist.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.github.pustike.persist.FieldGroup;

/**
 * The entity metadata.
 */
public final class EntityData {
    private final Class<?> entityClass;
    private final String tableName;
    private final Set<FieldData> declaredFieldData;
    private Map<String, FieldData> fieldDataMap;
    private EntityData parentEntity;
    private FieldData idField, versionField;
    private String joinFieldGroup;
    private Map<String, Set<String>> fieldGroupData;

    EntityData(Class<?> entityClass, String tableName) {
        this.entityClass = entityClass;
        this.tableName = tableName;
        this.declaredFieldData = new LinkedHashSet<>();
    }

    /**
     * Get the entity class using which this metadata is created.
     * @return the entity class
     */
    public Class<?> getEntityClass() {
        return entityClass;
    }

    /**
     * Get the table name for this entity. It can be {@code null} if this is a super class.
     * @see #isSuperClass()
     * @return the name of the table
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Get the parent entity metadata, if present.
     * @return the parent entity metadata.
     */
    public EntityData getParentEntity() {
        return parentEntity;
    }

    void setParentEntity(EntityData entityData) {
        this.parentEntity = entityData;
    }

    /**
     * Get the @{@link io.github.pustike.persist.Id} field metadata of this entity.
     * @return the id field metadata of this entity
     */
    public FieldData getIdField() {
        return idField;
    }

    private void setIdField(FieldData fieldData) {
        if (this.idField != null) {
            throw new IllegalStateException("multiple @Id columns can not be present in an entity");
        }
        this.idField = fieldData;
    }

    /**
     * Get the @{@link io.github.pustike.persist.Version} field metadata of this entity.
     * @return the version field metadata of this entity
     */
    public FieldData getVersionField() {
        return versionField;
    }

    private void setVersionField(FieldData fieldData) {
        if (this.versionField != null) {
            throw new IllegalStateException("multiple @Version columns can not be present in an entity");
        }
        this.versionField = fieldData;
    }

    void addField(FieldData fieldData) {
        fieldData.validate();
        declaredFieldData.add(fieldData);
    }

    /**
     * Get a set of declared field metadata directly on this entity.
     * @return a set of field metadata declared
     */
    public Set<FieldData> getDeclaredFieldData() {
        return Collections.unmodifiableSet(declaredFieldData);
    }

    /**
     * Get the field metadata for the given field name
     * @param fieldName the name of the field
     * @return the corresponding field metadata
     */
    public FieldData getFieldData(String fieldName) {
        FieldData fieldData = fieldDataMap.get(fieldName);
        if (fieldData == null) {
            throw new RuntimeException("FieldData is not available for field: " + fieldName + " in : " + entityClass);
        }
        return fieldData;
    }

    void validate() {
        List<FieldData> fieldDataList = new ArrayList<>(declaredFieldData);
        if (parentEntity != null) {
            fieldDataList.addAll(parentEntity.getFieldData());
        }
        fieldDataList.sort((o1, o2) -> {
            ColumnType type1 = o1.getColumnType(), type2 = o2.getColumnType();
            return type1 == ColumnType.Id ? -1 : type2 == ColumnType.Id ? 1 : type1 == ColumnType.Version ? 1
                : type2 == ColumnType.Version ? -1 : o1.getColumnName().compareTo(o2.getColumnName());
        });
        Map<String, FieldData> _fieldDataMap = new LinkedHashMap<>();
        for (FieldData fieldData : fieldDataList) {
            if (fieldData.getColumnType() == ColumnType.Id) {
                setIdField(fieldData);
            } else if (fieldData.getColumnType() == ColumnType.Version) {
                setVersionField(fieldData);
            }
            _fieldDataMap.put(fieldData.getName(), fieldData);
        }
        if (idField == null && !isSuperClass()) {
            throw new IllegalStateException("The @Table must have an @Id field for table: " + tableName);
        }
        this.fieldDataMap = Collections.unmodifiableMap(_fieldDataMap);
        readFieldGroupData();
    }

    /**
     * Get a collection of all field metadata, including that of parent entity.
     * @return the collection of all field metadata
     */
    public Collection<FieldData> getFieldData() {
        return fieldDataMap.values();
    }

    /**
     * Check if this entity metadata is of a super class. {@code true} if the table name is {@code null}.
     * @return true if this metadata is from a super class
     */
    public boolean isSuperClass() {
        return tableName == null;
    }

    private void readFieldGroupData() {
        this.fieldGroupData = new HashMap<>();
        Set<String> fetchFieldNames = createFieldNames();
        for (FieldData fieldData : getFieldData()) {
            if (fieldData.isFetch()) {
                fetchFieldNames.add(fieldData.getName());
            }
        }
        this.fieldGroupData.put(null, fetchFieldNames);
        FieldGroup[] fieldGroups = entityClass.getDeclaredAnnotationsByType(FieldGroup.class);
        if (fieldGroups == null) {
            return;
        }
        Map<String, FieldGroup> fieldGroupMap = new LinkedHashMap<>();
        for (FieldGroup fieldGroup : fieldGroups) {
            if (fieldGroup.joinFetch()) {
                if (joinFieldGroup != null) {
                    throw new IllegalStateException("Fetch Group with joinFetch can only be used once!: " + tableName);
                }
                joinFieldGroup = fieldGroup.name();
            }
            FieldGroup oldValue = fieldGroupMap.put(fieldGroup.name(), fieldGroup);
            if (oldValue != null) {
                throw new IllegalStateException("Fetch Group name should be unique for: " + tableName);
            }
        }
        for (FieldGroup fieldGroup : fieldGroupMap.values()) {
            Set<String> fieldNames = createFieldNames();
            for (String include : fieldGroup.includes()) {
                Set<String> includeFieldNames = fieldGroupData.get(include);
                if (includeFieldNames == null) {
                    throw new IllegalStateException("Included Fetch Group: " + include + " not found on " + tableName);
                }
                fieldNames.addAll(includeFieldNames);
            }
            fieldNames.addAll(Arrays.asList(fieldGroup.fields()));
            fieldGroupData.put(fieldGroup.name(), Collections.unmodifiableSet(fieldNames));
        }
    }

    private Set<String> createFieldNames() {
        LinkedHashSet<String> fieldNames = new LinkedHashSet<>();
        fieldNames.add(idField.getName());
        if (versionField != null) {
            fieldNames.add(versionField.getName());
        }
        return fieldNames;
    }

    /**
     * Get a set fields to be fetched during join. @{@link io.github.pustike.persist.Id} field is included always.
     * @param joinGroup the join group to be used, can be {@code null}
     * @return the set of join fetch fields
     */
    public Set<String> getJoinFetchFields(String joinGroup) {
        Set<String> joinFetchFields = new LinkedHashSet<>();
        joinFetchFields.add(idField.getName());
        String fieldGroup = joinGroup == null ? this.joinFieldGroup : joinGroup;
        if (fieldGroup != null) {
            joinFetchFields.addAll(getFieldGroupFields(fieldGroup));
        }
        return Collections.unmodifiableSet(joinFetchFields);
    }

    /**
     * Get the set of fields for the given fieldGroup, declared on this entity.
     * @param fieldGroup the name of the fieldGroup
     * @return the set of field names defined in this group
     */
    public Set<String> getFieldGroupFields(String fieldGroup) {
        Set<String> fieldGroupFields = fieldGroupData.get(fieldGroup);
        if (fieldGroupFields == null) {
            throw new IllegalStateException("Field Group: " + fieldGroup + " not found on " + tableName);
        }
        return fieldGroupFields;
    }
}
