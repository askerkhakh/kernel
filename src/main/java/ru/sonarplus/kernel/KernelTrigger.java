package ru.sonarplus.kernel;

import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.data_change_service.CommitDataChangeEventParameters;
import ru.sonarplus.kernel.data_change_service.DataChangeOperation;
import ru.sonarplus.kernel.observer.Event;
import ru.sonarplus.kernel.recordset.Field;
import ru.sonarplus.kernel.recordset.FieldValueFactory;
import ru.sonarplus.kernel.recordset.Record;

import javax.annotation.Nullable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * KernelTrigger
 */
class KernelTrigger {

    private static final String EXCLUDED_TECH_FIELDS_OPTION = "HT9IAYOJY_88UH1TOLQ_75YM3N9Q";
    private static final char EXCLUDED_TECH_FIELDS_DELIMITER = (char) 1;
    private static final String EXCLUDED_TECH_FIELDS_ALL = "*";

    private static final String INSERTION_DATE_FIELD_NAME = "ДатаПост.";
    private static final String INSERTION_TIME_FIELD_NAME = "ВремяПост.";
    private static final String INSERTION_RESPONSIBLE_FIELD_NAME = "КтоВвел";
    private static final String EDITING_DATE_FIELD_NAME = "ДатаИзмен.";
    private static final String EDITING_TIME_FIELD_NAME = "ВремяИзмен";
    private static final String EDITING_RESPONSIBLE_FIELD_NAME = "Ответств.";

    static void trigger(Event e) {
        CommitDataChangeEventParameters parameters = (CommitDataChangeEventParameters) e.getParameters();
        switch (parameters.operation) {
            case INSERT:
            case MODIFY:
                fillTechFields(
                        parameters.session.getUser(),
                        parameters.record,
                        (parameters.operation == DataChangeOperation.INSERT),
                        getExcludedFieldsOption(parameters.options)
                );
        }
    }

    @Nullable
    private static String getExcludedFieldsOption(@Nullable Map<String, String> options) {
        if (options == null) {
            return null;
        }
        return options.get(EXCLUDED_TECH_FIELDS_OPTION);
    }

    private static void fillTechFields(User user, Record row, boolean inserting, @Nullable String excludedFieldsOption) {
        Set<String> excludedFields = null;
        if (excludedFieldsOption != null) {
            if (excludedFieldsOption.equals(EXCLUDED_TECH_FIELDS_ALL)) {
                return;
            }
            excludedFields = new HashSet<>(Arrays.asList(StringUtils.split(excludedFieldsOption, EXCLUDED_TECH_FIELDS_DELIMITER)));
        }

        Field insertionDateField = null;
        Field insertionTimeField = null;
        Field insertionResponsibleField = null;
        if (inserting) {
            insertionDateField = tryGetTechField(row, excludedFields, INSERTION_DATE_FIELD_NAME);
            insertionTimeField = tryGetTechField(row, excludedFields, INSERTION_TIME_FIELD_NAME);
            insertionResponsibleField = tryGetTechField(row, excludedFields, INSERTION_RESPONSIBLE_FIELD_NAME);
        }

        Field editingDateField = tryGetTechField(row, excludedFields, EDITING_DATE_FIELD_NAME);
        Field editingTimeField = tryGetTechField(row, excludedFields, EDITING_TIME_FIELD_NAME);
        Field editingResponsibleField = tryGetTechField(row, excludedFields, EDITING_RESPONSIBLE_FIELD_NAME);

        if ((insertionDateField != null) || (editingDateField != null)) {
            LocalDate date = LocalDate.now();
            if (insertionDateField != null) {
                insertionDateField.setValue(FieldValueFactory.ofDate(date));
            }
            if (editingDateField != null) {
                editingDateField.setValue(FieldValueFactory.ofDate(date));
            }
        }
        if ((insertionTimeField != null) || (editingTimeField != null)) {
            LocalTime time = LocalTime.now();
            if (insertionTimeField != null) {
                insertionTimeField.setValue(FieldValueFactory.ofTime(time));
            }
            if (editingTimeField != null) {
                editingTimeField.setValue(FieldValueFactory.ofTime(time));
            }
        }

        if (insertionResponsibleField != null) {
            insertionResponsibleField.setValue(FieldValueFactory.ofLong(user.getId()));
        }
        if (editingResponsibleField != null) {
            editingResponsibleField.setValue(FieldValueFactory.ofLong(user.getId()));
        }
    }

    @Nullable
    private static Field tryGetTechField(Record record, @Nullable Set<String> excludedFields, String fieldName) {
        if ((excludedFields != null) && excludedFields.contains(fieldName)) {
            return null;
        }
        else {
            return record.tryGetFieldByName(fieldName);
        }
    }

}