package ru.sonarplus.kernel.sqlobject.db_support;

import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.common_utils.DoubleQuoteUtils;
import ru.sonarplus.kernel.sqlobject.common_utils.RecIdValueWrapper;
import ru.sonarplus.kernel.sqlobject.db_support_ora.SqlObjectsDbSupportUtilsOra;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.Conditions;
import ru.sonarplus.kernel.sqlobject.objects.Expression;
import ru.sonarplus.kernel.sqlobject.objects.FromClauseItem;
import ru.sonarplus.kernel.sqlobject.objects.GroupBy;
import ru.sonarplus.kernel.sqlobject.objects.Join;
import ru.sonarplus.kernel.sqlobject.objects.OraFTSMarker;
import ru.sonarplus.kernel.sqlobject.objects.OrderByItem;
import ru.sonarplus.kernel.sqlobject.objects.ParamRef;
import ru.sonarplus.kernel.sqlobject.objects.PredicateForCodeComparison;
import ru.sonarplus.kernel.sqlobject.objects.PredicateFullTextSearch;
import ru.sonarplus.kernel.sqlobject.objects.PredicateRegExpMatch;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedField;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedRField;
import ru.sonarplus.kernel.sqlobject.objects.QueryParam;
import ru.sonarplus.kernel.sqlobject.objects.QueryParams;
import ru.sonarplus.kernel.sqlobject.objects.Select;
import ru.sonarplus.kernel.sqlobject.objects.SelectedColumn;
import ru.sonarplus.kernel.sqlobject.objects.SelectedColumnsContainer;
import ru.sonarplus.kernel.sqlobject.objects.SourceTable;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.SqlQuery;
import ru.sonarplus.kernel.sqlobject.objects.SqlTransactionCommand;
import ru.sonarplus.kernel.sqlobject.objects.TupleExpressions;
import ru.sonarplus.kernel.sqlobject.objects.UnionItem;
import ru.sonarplus.kernel.sqlobject.objects.UnionsContainer;
import ru.sonarplus.kernel.sqlobject.objects.Value;
import ru.sonarplus.kernel.sqlobject.objects.ValueRecId;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public class SqlObjectsConvertorDebug extends SqlObjectsConvertor {

    public SqlObjectsConvertorDebug() { super(true); }

    public static String dc(SqlObject item) {
        return debugconvert(item);
    }

    public static String debugconvert(SqlObject item) {
        try {
            return new SqlObjectsConvertorDebug().convert(item);
        }
        catch (Exception e) {
            return (item == null ? "<null>" : item.getClass().getSimpleName()) + ": " + e.toString();
        }
    }

    @Override
    protected SqlObjectsDbSupportUtils createDBSupport() {
        // TODO для отладочного sql-конвертора сейчас используем модуль поддержки Oracle...
        return new SqlObjectsDbSupportUtilsOra();
    }

    @Override
    protected void setupConvertParams(ConvertParams params) {}

    @Override
    protected String internalConvertItem(SqlObject item, ConvertParams params, ConvertState state){

        if (item == null)
            return "<NULL>";

        String result;
        try {
            if (item.getClass() == OraFTSMarker.class)
                result =  OraFTSMarker.class.getSimpleName();
            else if (item.getClass() == QueryParam.class)
                result =  convertQueryParam((QueryParam) item, params, state);
            else if (item.getClass() == PredicateForCodeComparison.class)
                result =  convertPredicateCodeCmp((PredicateForCodeComparison) item, params, state);
            else if (item.getClass() == PredicateFullTextSearch.class)
                result =  convertPredicateFTS((PredicateFullTextSearch) item, params, state);
            else if (item.getClass() == PredicateRegExpMatch.class)
                result =  convertPredicateRegExp((PredicateRegExpMatch) item, params, state);
            else if (item.getClass() == QualifiedRField.class)
                result =  convertQRField((QualifiedRField)item, params, state);
            else if (item.getClass() == QueryParams.class)
                result =  convertQueryParams((QueryParams) item, params, state);
            else if (item.getClass() == SqlTransactionCommand.class)
                result =  convertTransactionCommand((SqlTransactionCommand)item, params, state);
            else if (item.getClass() == ValueRecId.class)
                result =  convertValueRecId((ValueRecId) item, params, state);
            else
                result =  super.internalConvertItem(item, params, state);
        }
        catch (Exception e) {
            result = item.getClass().getSimpleName() + ' ' + e.toString();
        }
        if (item instanceof SqlQuery) {
            QueryParams queryParams = ((SqlQuery)item).getParams();
            if (queryParams != null)
                try {
                    result = convert(queryParams, params, state) + result;
                }
                catch (Exception e) {
                    result = '(' + queryParams.getClass().getSimpleName() + ':' + e.toString() + ')' + result;
                }
        }
        return result;
    }

    @Override
    protected String convertExpression(Expression item, ConvertParams params, ConvertState state)
            throws Exception{

        StringBuilder sb = new StringBuilder();
        sb.append('(');
        if (!item.isPureSql)
            sb.append("(isPureSql: False)");
        String expr = item.getExpr();
        sb.append("(expr: <");
        if (StringUtils.isEmpty(expr))
            sb.append("...");
        else
            sb.append(expr);
        sb.append(">");
        if (item.isHasChilds()) {
            sb.append(", args: <");
            boolean isFirst = true;
            for (SqlObject child: item) {
                if (!isFirst)
                    sb.append(", ");
                sb.append(convert(child, params, state));
                isFirst = false;
            }
            sb.append('>');
        }
        sb.append(')');
        return sb.toString();
    }

    @Override
    protected String convertTransactionCommand(SqlTransactionCommand item, ConvertParams params, ConvertState state) {
        return item.command.toString() + (StringUtils.isEmpty(item.statement) ? "" : " " + item.statement);
    }

    protected String convertQRField(QualifiedField item, ConvertParams params, ConvertState state) {
        return "r`" + (!StringUtils.isEmpty(item.alias) ? item.alias + "." : "") + (!StringUtils.isEmpty(item.fieldName) ? item.fieldName : "RecordId()");
    }

    @Override
    protected String convertQField(QualifiedField item, ConvertParams params, ConvertState state)
            throws SqlObjectsConvertorException{
        if (SqlObjectUtils.isAsterisk(item))
            return (!StringUtils.isEmpty(item.alias) ? item.alias + "." : "") + '*';

        if (SqlObjectUtils.isRecordId(item))
            return (!StringUtils.isEmpty(item.alias) ? item.alias + "." : "") + "RecordId()";

        return super.convertQField(item, params, state);
    }

    protected String matchParammeter(PredicateRegExpMatch predicate) {
        StringBuilder result = new StringBuilder();
        if (predicate.caseSensitive) {
            result.append('c');
        }
        else {
            result.append('i');
        }
        if (predicate.pointAsCRLF) {
            result.append('n');
        }
        if (predicate.multiLine) {
            result.append('m');
        }
        return result.toString();
    }

    protected String convertPredicateRegExp(PredicateRegExpMatch item, ConvertParams params, ConvertState state)
            throws Exception {
        return (item.not ? "NOT " : "") + "REGEXP_LIKE(" + convert(item.getLeft(), params, state)+
                "," + convert(item.getRight(), params, state) +
                "," +
                "'"+ matchParammeter(item).replaceAll("'","''")+"')";
    }

    protected String convertPredicateFTS(PredicateFullTextSearch item, ConvertParams params, ConvertState state)
            throws Exception {
        return (item.not ? "NOT " : "") + "FTS(" + convert(item.getLeft(), params, state)+
                "," + convert(item.getRight(), params, state) +
                ")";
    }

    protected String convertPredicateCodeCmp(PredicateForCodeComparison item, ConvertParams params, ConvertState state)
            throws Exception {
        return (item.not ? "NOT " : "") + "CodeCompare(" +
                convert(item.getLeft(), params, state)+ ',' +
                item.comparison.toString() + ',' +
                convert(item.getRight(), params, state) +
                ")";
    }

    protected String convertQueryParams(QueryParams item, ConvertParams params, ConvertState state)
            throws Exception {
        String result = "";
        for (SqlObject child: item) {
            if (!StringUtils.isEmpty(result))
                result = result + ", ";
            result = result + convert(child, params, state);
        }
        if (!StringUtils.isEmpty(result))
            result = "QueryParams(" + result + ')';
        return result;
    }

    protected String convertQueryParam(QueryParam item, ConvertParams params, ConvertState state)
            throws Exception{
        String result = "(:" + (!StringUtils.isEmpty(item.parameterName) ? item.parameterName : "<...>") + ',';
        result = result + item.getParamType().toString() + ',';
        result = result + item.getValueType().toString() + ',';
        Value value = item.getValueObj();
        if (value == null)
            result = result + "<no value>";
        else
            result = result + convert(value, params, state);
        result = result + ')';
        return result;
    }

    protected QueryParam findQueryParam(SqlObject start, ParamRef paramRef) {
        SqlQuery parent = SqlObjectUtils.findParentQuery(start);
        if (parent == null)
            return null;
        QueryParam param = null;
        QueryParams params = parent.getParams();
        if (params != null)
            param = params.findParam(paramRef.parameterName);
        if (param != null)
            return param;
        return findQueryParam(parent, paramRef);
    }

    @Override
    protected String convertParamRef(ParamRef item, ConvertParams params, ConvertState state)
            throws Exception{
        if (StringUtils.isEmpty(item.parameterName))
            return ":<...>";
        QueryParam param = findQueryParam(item, item);
        if (param != null)
            return ":" + convertQueryParam(param, params, state);
        return ":" + DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(item.parameterName);
    }

    protected String convertValueRecId(ValueRecId item, ConvertParams params, ConvertState state) {
        String result = "vrecid(";
        RecIdValueWrapper recId = item.getValue();
        for (int i = 0; i < recId.count(); i++) {
            Object v = recId.getValue(i);
            if (v == null)
                result = result + "null" + '|';
            else
                try {
                    result = result + v.toString() + '|';
                } catch (Exception e) {
                    result = result + v.getClass().getSimpleName() + ':' + e.getMessage() + '|';
                }
        }
        result = result + ')';
        return result;
    }

    @Override
    protected String convertPredicateBracket(Conditions item, ConvertParams params, ConvertState state)
            throws Exception{
        StringBuilder sb = new StringBuilder();

        String booleanOp;
        switch (item.booleanOp) {
            case AND:
                booleanOp = " AND ";
                break;
            case OR:
                booleanOp = " OR ";
                break;
            default:
                booleanOp = item.booleanOp.toString();
        }

        if (item.not)
            sb.append("NOT");
        sb.append('(');
        sb.append("/*");
        sb.append(booleanOp);
        sb.append("*/");

        boolean isFirst = true;
        for (SqlObject child: item) {
            if (!isFirst)
                sb.append(booleanOp);
            sb.append(convert(child, params, state));
            isFirst = false;
        }

        sb.append(')');
        return sb.toString();
    }

    @Override
    protected String convertSourceTable(SourceTable item, ConvertParams params, ConvertState state){
        if (StringUtils.isEmpty(item.table))
            return "<...>";
        return DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(item.table);
    }

    @Override
    protected String convertFromItem(FromClauseItem item, ConvertParams params, ConvertState state)
            throws Exception {
        StringBuilder sb = new StringBuilder();
        if (item.getIsJoined()) {
            Join join = item.getJoin();
            if (join == null)
                sb.append("<no join info>");
            else {
                sb.append(join.joinType.toString());
                sb.append(" ");
                sb.append(convert(item.getTableExpr()));
                sb.append(" ON ");
                sb.append(convert(join.getJoinOn(), params, state));
            }
        }
        else
            sb.append(convert(item.getTableExpr()));

        return sb.toString();
    }

    @Override
    protected String convertUnionItem(UnionItem item, ConvertParams params, ConvertState state)
            throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(' ');
        sb.append(unionTypeToString(item.unionType).toUpperCase());
        sb.append(' ');
        UnionsContainer unions = null;
        Select select = item.findSelect();
        if (select != null)
            unions = select.findUnions();
        if (unions != null && unions.isHasChilds()) {
            sb.append('(');
            sb.append(convert(select, params, state));
            sb.append(')');
        } else
            sb.append(convert(select, params, state));
        return sb.toString();
    }

    @Override
    protected String convertGroupBy(GroupBy item, ConvertParams params, ConvertState state)
            throws Exception {
        TupleExpressions tuple = item.findTuple();
        Conditions having = item.getHaving();
        StringBuilder sb = new StringBuilder(" GROUP BY ");
        if (tuple == null || !tuple.isHasChilds())
            sb.append("<...>");
        else {
            boolean isFirst = true;
            for (SqlObject child : tuple) {
                if (!isFirst)
                    sb.append(',');
                sb.append(convert(child, params, state));
                isFirst = false;
            }
            if (having != null && !having.isEmpty()) {
                sb.append(" HAVING ");
                sb.append(convert(having, params, state));
            }
        }
        return sb.toString();
    }

    @Override
    protected String convertColumn(SelectedColumn item, ConvertParams params, ConvertState state)
            throws Exception {
        ColumnExpression expr = item.getColExpr();
        return (expr == null ? "<...>" : convert(expr, params, state)) +
                (!StringUtils.isEmpty(item.alias) ? " " + DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(item.alias) : "");
    }

    @Override
    protected String convertColumns(SelectedColumnsContainer item, ConvertParams params, ConvertState state)
            throws Exception {
        if (!item.isHasChilds())
            return "<...>";

        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (SqlObject child: item) {
            if (!isFirst)
                sb.append(',');
            sb.append(convert(child, params, state));
            isFirst = false;
        }
        return sb.toString();
    }

    @Override
    protected String orderDirectionToStr(OrderByItem.OrderDirection direction) {
        switch (direction) {
            case ASC:
                return "asc";
            case DESC:
                return "desc";
            default:
                return direction.toString();
        }
    }

    @Override
    protected String nullOrderingToStr(OrderByItem.NullOrdering null_ordering) {
        switch (null_ordering) {
            case NULLS_FIRST:
                return " nulls first";
            case NULLS_LAST:
                return " nulls last";
            default:
                return null_ordering.toString();
        }
    }

}
