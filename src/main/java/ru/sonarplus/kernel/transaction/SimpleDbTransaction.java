package ru.sonarplus.kernel.transaction;

import ru.sonarplus.kernel.ClientSession;
import ru.sonarplus.kernel.SqlObjectExecutionService;
import ru.sonarplus.kernel.sqlobject.objects.SqlTransactionCommand;

/**
 * Реализация интерфейса транзакции, которая стартует и финиширует обычную транзакцию
 */
class SimpleDbTransaction implements DbTransaction {

    private final ClientSession session;
    private final SqlObjectExecutionService commandExecutionService;

    SimpleDbTransaction(ClientSession session, SqlObjectExecutionService commandExecutionService) {
        this.session = session;
        this.commandExecutionService = commandExecutionService;
    }

    @Override
    public void start() {
        // TODO #45201 для SQLite выполнять здесь старт транзакции (см 41628 и связанное с ней обсуждение в zulip)
        // commandExecutionService.executeCommand(session, SqlTransactionCommand.newSetTransactionCommand());
    }

    @Override
    public void commit() throws Exception {
        commandExecutionService.executeCommand(session, SqlTransactionCommand.newCommitCommand());
    }

    @Override
    public void rollback() throws Exception {
        commandExecutionService.executeCommand(session, SqlTransactionCommand.newRollbackCommand());
    }
}
