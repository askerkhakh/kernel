package ru.sonarplus.kernel.data_change_service;

import ru.sonarplus.kernel.ClientSession;
import ru.sonarplus.kernel.observer.Observer;
import ru.sonarplus.kernel.recordset.TableRecord;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * DataChangeService - сервис поддерживающий изменение данных в таблице (добавление, редактирование и удаление записи). 
 * 
 * Поддерживает механизм триггеров - для всех операций изменения данных, производимых через этот 
 * сервис, будут срабатывать триггеры, как табличные, так и общие.
 */
public interface DataChangeService {

    @Nullable
    Long insertRecord(ClientSession session, TableRecord row, @Nullable Map<String, String> options) throws Exception;

    boolean modifyRecord(ClientSession session, TableRecord row, @Nullable Map<String, String> options) throws Exception;

    boolean deleteRecord(ClientSession session, TableRecord row, @Nullable Map<String, String> options) throws Exception;


    void registerGeneralTrigger(Observer trigger);

    void registerTableTrigger(String tableName, Observer tableTrigger);
}