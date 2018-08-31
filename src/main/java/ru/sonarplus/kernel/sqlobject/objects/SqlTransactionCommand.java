package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class SqlTransactionCommand extends SqlObject {

    public enum TransactionCommand {
		SET_TRANSACTION, ROLLBACK, COMMIT,
		SET_SAVEPOINT, ROLLBACK_TO, REMOVE_SAVEPOINT
	}

	public TransactionCommand command;
	public String statement;

    public SqlTransactionCommand() { super(); }

    public static SqlTransactionCommand newCommitCommand() {
        SqlTransactionCommand command = new SqlTransactionCommand();
        command.command = TransactionCommand.COMMIT;
        return command;
    }

    public static SqlTransactionCommand newSetTransactionCommand() {
        SqlTransactionCommand command = new SqlTransactionCommand();
        command.command = TransactionCommand.COMMIT;
        return command;
    }

    public static SqlTransactionCommand newRollbackCommand() {
        SqlTransactionCommand command = new SqlTransactionCommand();
        command.command = TransactionCommand.ROLLBACK;
        return command;
    }

    public static SqlTransactionCommand newSetSavepointCommand(String savepointName) {
        SqlTransactionCommand command = new SqlTransactionCommand();
        command.command = TransactionCommand.SET_SAVEPOINT;
        command.statement = savepointName;
        return command;
    }

    public static SqlTransactionCommand newRollbackToCommand(String savepointName) {
        SqlTransactionCommand command = new SqlTransactionCommand();
        command.command = TransactionCommand.ROLLBACK_TO;
        command.statement = savepointName;
        return command;
    }

    @Deprecated
	public SqlTransactionCommand(SqlObject owner)
            throws SqlObjectException {
		super(owner);
	}
	
	public static void checkTransactionCommand(SqlTransactionCommand command) {
		if ((command.command == TransactionCommand.SET_SAVEPOINT) ||
		(command.command == TransactionCommand.ROLLBACK_TO) || 
		(command.command == TransactionCommand.REMOVE_SAVEPOINT)) {
			Preconditions.checkArgument(!StringUtils.isEmpty(command.statement),
					"Для команды (%s) необходимо указывать имя точки сохранения",
					command.command.toString());
		}
	}
}
