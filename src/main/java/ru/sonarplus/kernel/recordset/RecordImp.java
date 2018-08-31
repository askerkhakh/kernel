package ru.sonarplus.kernel.recordset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordImp implements Record {

    private final Map<String, Field> fieldsByName = new HashMap<>();
    private final List<Field> fields = new ArrayList<>();

    @Override
    public Field tryGetFieldByName(String fieldName) {
        return fieldsByName.get(fieldName);
    }

    @Override
    public Iterable<Field> getFields() {
        return fieldsByName.values();
    }

    @Override
    public int getFieldCount() {
        return fields.size();
    }

    @Override
    public Field getField(int index) {
        return fields.get(index);
    }

    protected void addField(String fieldName, Field field) {
        fields.add(field);
        fieldsByName.put(fieldName, field);
    }

    protected void addField(Field field) {
        addField(field.getName(), field);
    }

}
