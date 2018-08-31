package ru.sonarplus.kernel;

/**
 * Сервис, позволяющий получить следующее значение последовательности для таблицы.
 */
public interface DbSequenceService {

    long getNext(ClientSession session, String tableName) throws Exception;

}
