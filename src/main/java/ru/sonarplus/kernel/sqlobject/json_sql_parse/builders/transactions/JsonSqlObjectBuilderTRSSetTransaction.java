package ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.transactions;

import org.json.simple.JSONObject;
import ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilder;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.SqlTransactionCommand;

import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KEY_TRS_STMT;
import static ru.sonarplus.kernel.sqlobject.json_sql_parse.builders.JsonSqlObjectBuilderConsts.KIND_TRS_SET_TRANSACTION;

public class JsonSqlObjectBuilderTRSSetTransaction extends JsonSqlObjectBuilder {
    @Override
    public SqlObject parseJsonNode(JSONObject node) {
        SqlTransactionCommand trs = new SqlTransactionCommand();
        trs.command = SqlTransactionCommand.TransactionCommand.SET_TRANSACTION;
        trs.statement = (String) node.get(KEY_TRS_STMT);
        return trs;
    }

    @Override
    public String getKind() { return KIND_TRS_SET_TRANSACTION; }

}
