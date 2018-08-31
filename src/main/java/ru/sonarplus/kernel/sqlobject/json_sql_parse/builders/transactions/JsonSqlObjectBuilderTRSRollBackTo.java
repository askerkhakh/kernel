package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.transactions;

import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.SqlTransactionCommand;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_TRS_SAVEPOINT_NAME;
import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KIND_TRS_ROLLBACK_TO;

public class JsonSqlObjectBuilderTRSRollBackTo extends JsonSqlObjectBuilder {
    @Override
    public SqlObject parseJsonNode(JSONObject node) {
        SqlTransactionCommand trs = new SqlTransactionCommand();
        trs.command = SqlTransactionCommand.TransactionCommand.ROLLBACK_TO;
        trs.statement = (String) node.get(KEY_TRS_SAVEPOINT_NAME);
        return trs;
    }

    @Override
    public String getKind() { return KIND_TRS_ROLLBACK_TO; }

}
