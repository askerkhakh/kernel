package ru.sonarplus.kernel.transaction;

import ru.sonarplus.kernel.ClientSession;
import ru.sonarplus.kernel.SqlObjectExecutionService;
import ru.sonarplus.kernel.sqlobject.objects.SqlTransactionCommand;

/**
 * Реализация интерфейса транзакции, которая использует SAVEPOINT'ы
 */
class SavepointDbTransaction implements DbTransaction {
    private final ClientSession session;
    private final SqlObjectExecutionService commandExecutionService;
    private final int id;

    SavepointDbTransaction(ClientSession session, SqlObjectExecutionService commandExecutionService, int id) {
        this.session = session;
        this.commandExecutionService = commandExecutionService;
        this.id = id;
    }

    private String getSavePointName() {
        return String.format("SP%d", id);
    }

    @Override
    public void start() throws Exception {
        commandExecutionService.executeCommand(session, SqlTransactionCommand.newSetSavepointCommand(getSavePointName()));
    }

    @Override
    public void commit() {
        // ничего не делаем - SAVEPOINT будет удалён при финише охватывающей транзакции
    }

    @Override
    public void rollback() throws Exception {
        commandExecutionService.executeCommand(session, SqlTransactionCommand.newRollbackToCommand(getSavePointName()));
    }
}
