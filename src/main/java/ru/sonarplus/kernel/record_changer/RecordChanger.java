package ru.sonarplus.kernel.record_changer;

/**
 * �����, ����������� ��������������� ������������ ������, ���� �������� �����.
 */
public interface RecordChanger {

    void setFieldValue(String fieldName, Object fieldValue);

    void save() throws Exception;

}
