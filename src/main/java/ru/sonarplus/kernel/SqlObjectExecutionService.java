package ru.sonarplus.kernel;

import ru.sonarplus.kernel.recordset.RecordSet;
import ru.sonarplus.kernel.sqlobject.objects.CursorSpecification;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

/**
 * SqlObjectExecutionService
 */
public interface SqlObjectExecutionService {

    /**
     * Выполнить slqobject-команду command в рамках сессии session
     *
     * @param session сессия, в рамках которой выполняется команда
     * @param command выполняемая команда
     * @return возвращается количество записей, затронутых командой
     */
    int executeCommand(ClientSession session, SqlObject command) throws Exception;

    /**
     * Выполнить запрос cursorSpecification в рамках сессии session
     * @param session сессия, в рамках которой выполняется команда
     * @param cursorSpecification выполняемый запрос
     * @return возвращаемый набор записей
     */
    RecordSet executeCursor(ClientSession session, CursorSpecification cursorSpecification) throws Exception;
}