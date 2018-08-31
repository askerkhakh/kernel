package ru.sonarplus.kernel.record_changer;

/**
 *  ласс, позвол€ющий отредактировать существующую запись, либо добавить новую.
 */
public interface RecordChanger {

    void setFieldValue(String fieldName, Object fieldValue);

    void save() throws Exception;

}
