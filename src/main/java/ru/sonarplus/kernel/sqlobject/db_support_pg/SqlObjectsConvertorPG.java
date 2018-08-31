package ru.sonarplus.kernel.sqlobject.db_support_pg;

import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsConvertor;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsDbSupportUtils;
import ru.sonarplus.kernel.sqlobject.objects.*;

public class SqlObjectsConvertorPG extends SqlObjectsConvertor {

    public SqlObjectsConvertorPG() { super(true); }

    @Override
    protected SqlObjectsDbSupportUtils createDBSupport() { return new SqlObjectsDBSupportUtilsPG(); }

    @Override
    protected String convertCall(CallStoredProcedure item, ConvertParams params, ConvertState state)
            throws Exception{
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append('"');
        sb.append(item.spName);
        sb.append('"');

        sb.append('(');
        TupleExpressions tuple = item.findTuple();
        if (tuple != null && tuple.isHasChilds()) {
            boolean isFirst = true;
            for (SqlObject child: tuple) {
                if (!isFirst)
                    sb.append(',');
                sb.append(convert(child, params, state));
                isFirst = false;
            }
        }
        sb.append(')');
        return sb.toString();
    }

}
