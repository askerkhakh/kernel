package ru.sonarplus.kernel;

/**
 * ������, ����������� �������� ��������� �������� ������������������ ��� �������.
 */
public interface DbSequenceService {

    long getNext(ClientSession session, String tableName) throws Exception;

}
