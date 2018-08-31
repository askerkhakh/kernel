package ru.sonarplus.kernel.sqlobject.db_support;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.DoubleQuoteUtils;
import ru.sonarplus.kernel.sqlobject.expressions.ExprUtils;
import ru.sonarplus.kernel.sqlobject.expressions.ExpressionException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.HashMap;
import java.util.Map;

public abstract class SqlObjectsConvertor {

    public SqlObjectsConvertor(boolean useStandardNulls) {
        this.useStandardNulls = useStandardNulls;
    }

    public static class ConvertParams {
        private boolean useJDbcParamRef = true;
        public ConvertParams() {}
        public ConvertParams useJDbcParamRef(boolean value) { this.useJDbcParamRef = value; return this; }
        public boolean useJDbcParamRef(){ return this.useJDbcParamRef; }
    }
    public static class ConvertState {
        private SqlObject rootItem = null;

        public ConvertState() {}

        public void setRootItem(SqlObject item) {
            Preconditions.checkState(this.rootItem == null);
            this.rootItem = item;
        }
        public SqlObject getRootItem() { return this.rootItem; }
        public SqlObject currentItem = null;

    }

    protected abstract SqlObjectsDbSupportUtils createDBSupport();
    protected void setupConvertParams(ConvertParams params) {}
    protected Class getConvertParamsClass() { return ConvertParams.class; }
    protected Class getConvertStateClass() { return ConvertState.class; }

    public static class SqlObjectsConvertorException extends SqlObjectException {
        public SqlObjectsConvertorException(String message) {
            super(message);
        }
    }
    private SqlObjectsDbSupportUtils dbSupport = null;

    public SqlObjectsDbSupportUtils getDBSupport() {
        if (dbSupport == null)
            dbSupport = createDBSupport();
        return dbSupport;
    }

    private ConvertParams defaultConvertParams = null;

    protected final ConvertParams getDefaultConvertParams() {
        try {
            if (this.defaultConvertParams == null) {
                this.defaultConvertParams = (ConvertParams) getConvertParamsClass().newInstance();
                setupConvertParams(this.defaultConvertParams);
            }
        }
        catch (IllegalAccessException|InstantiationException e) {
            throw new RuntimeException(e);
        }
        return this.defaultConvertParams;
    }

    private final boolean useStandardNulls;

    public boolean getUseStandardNulls() {
        return useStandardNulls;
    }

    protected ConvertState createConvertContext(SqlObject item) {
        try {
            ConvertState result = (ConvertState) getConvertStateClass().newInstance();
            result.setRootItem(item);
            return result;
        }
        catch (IllegalAccessException|InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    public final String convert(SqlObject item) {
        return convert(item, getDefaultConvertParams(), createConvertContext(item));
    }

    public final String convert(SqlObject item, ConvertParams params)
            throws Exception{
        ConvertParams lparams = params;
        if (lparams == null)
            lparams = getDefaultConvertParams();
        return convert(item, lparams, createConvertContext(item));
    }

    protected final String convert(SqlObject item, ConvertParams params, ConvertState state) {
        state.currentItem = item;
        try {
            return internalConvertItem(item, params, state);
        }
        catch (SqlObjectsConvertorException e) {
            throw e;
        }
        catch (Exception e) {
            throw new SqlObjectsConvertorException(String.format("%s: %s", e.getClass().getSimpleName(), e.getMessage()));
        }
    }

    protected String internalConvertItem(SqlObject item, ConvertParams params, ConvertState state) throws Exception {

        if (item == null)
            throw new SqlObjectsConvertorException("null");

        if (item.getClass() == QualifiedField.class)
            return convertQField((QualifiedField)item, params, state);

        if (item.getClass() == Expression.class)
            return convertExpression((Expression)item, params, state);

        if (item.getClass() == CaseSimple.class)
            return convertCaseSimple((CaseSimple)item, params, state);

        if (item.getClass() == CaseSearch.class)
            return convertCaseSearch((CaseSearch)item, params, state);

        if (item.getClass() == ParamRef.class)
            return convertParamRef((ParamRef)item, params, state);

        if (item.getClass() == Scalar.class)
            return convertScalar((Scalar)item, params, state);

        if (item.getClass() == ValueConst.class)
            return convertValueConst((ValueConst)item, params, state);

        if (item.getClass() == PredicateComparison.class)
            return convertPredicateComparison((PredicateComparison)item, params, state);

        if (item.getClass() == PredicateBetween.class)
            return convertPredicateBetween((PredicateBetween)item, params, state);

        if (item.getClass() == PredicateExists.class)
            return convertPredicateExists((PredicateExists)item, params, state);

        if (item.getClass() == PredicateLike.class)
            return convertPredicateLike((PredicateLike)item, params, state);

        if (item.getClass() == PredicateInTuple.class)
            return convertPredicateInTuple((PredicateInTuple)item, params, state);

        if (item.getClass() == PredicateInQuery.class)
            return convertPredicateInSelect((PredicateInQuery)item, params, state);

        if (item.getClass() == PredicateIsNull.class)
            return convertPredicateIsNull((PredicateIsNull)item, params, state);

        if (item.getClass() == Conditions.class)
            return convertPredicateBracket((Conditions)item, params, state);

        if (item.getClass() == CursorSpecification.class)
            return convertCursorSpec((CursorSpecification)item, params, state);

        if (item.getClass() == Select.class)
            return convertSelect((Select)item, params, state);

        if (item.getClass() == SqlQueryInsert.class)
            return convertInsert((SqlQueryInsert)item, params, state);

        if (item.getClass() == SqlQueryUpdate.class)
            return convertUpdate((SqlQueryUpdate)item, params, state);

        if (item.getClass() == SqlQueryDelete.class)
            return convertDelete((SqlQueryDelete)item, params, state);

        if (item.getClass() == CallStoredProcedure.class)
            return convertCall((CallStoredProcedure)item, params, state);

        if (item.getClass() == FromContainer.class)
            return convertFrom((FromContainer)item, params, state);

        if (item.getClass() == FromClauseItem.class)
            return convertFromItem((FromClauseItem)item, params, state);

        if (item.getClass() == TableExpression.class)
            return convertTableExpr((TableExpression)item, params, state);

        if (item.getClass() == SourceTable.class)
            return convertSourceTable((SourceTable)item, params, state);

        if (item.getClass() == SourceQuery.class)
            return convertSourceSelect((SourceQuery)item, params, state);

        if (item.getClass() == CTEsContainer.class)
            return convertWith((CTEsContainer)item, params, state);

        if (item.getClass() == CommonTableExpression.class)
            return convertCTE((CommonTableExpression)item, params, state);

        if (item.getClass() == SelectedColumnsContainer.class)
            return convertColumns((SelectedColumnsContainer)item, params, state);

        if (item.getClass() == SelectedColumn.class)
            return convertColumn((SelectedColumn)item, params, state);

        if (item.getClass() == GroupBy.class)
            return convertGroupBy((GroupBy)item, params, state);

        if (item.getClass() == OrderBy.class)
            return convertOrderBy((OrderBy)item, params, state);

        if (item.getClass() == OrderByItem.class)
            return convertOrderByItem((OrderByItem)item, params, state);

        if (item.getClass() == UnionsContainer.class)
            return convertUnions((UnionsContainer)item, params, state);

        if (item.getClass() == UnionItem.class)
            return convertUnionItem((UnionItem)item, params, state);

        if (item.getClass() == SqlTransactionCommand.class)
            return convertTransactionCommand((SqlTransactionCommand)item, params, state);
            
        throw new SqlObjectsConvertorException(
                String.format(
                        "%s: Конвертация в sql для %s не поддержана",
                        this.getClass().getSimpleName(),
                        item.getClass().getSimpleName()));
    }

    protected String convertTransactionCommand(SqlTransactionCommand item, ConvertParams params, ConvertState state) {
        SqlTransactionCommand.checkTransactionCommand(item);
        switch (item.command) {
            case SET_TRANSACTION:
                StringBuilder result = new StringBuilder("SET TRANSACTION");
                if (!StringUtils.isEmpty(item.statement)) {
                    result.append(' ');
                    result.append(item.statement);
                }
                return result.toString();
            case ROLLBACK:
                return "ROLLBACK";
            case COMMIT:
                return "COMMIT";
            case SET_SAVEPOINT:
                return "SAVEPOINT " + item.statement;
            case ROLLBACK_TO:
                return "ROLLBACK TO " + item.statement;
            case REMOVE_SAVEPOINT:
                return "RELEASE SAVEPOINT " + item.statement;
            default:
                Preconditions.checkArgument(false);
                return null;
        }
    }

    public String unionTypeToString(UnionItem.UnionType unionType) {
        switch (unionType) {
            case MINUS:
                return "EXCEPT";
            default:
                return unionType.toString();
        }
    }

    protected String orderDirectionToStr(OrderByItem.OrderDirection direction) {
        switch (direction) {
            case ASC:
                return "asc";
            case DESC:
                return "desc";
            default:
                Preconditions.checkArgument(false, "unknown order direction");
                return null;
        }
    }

    protected String nullOrderingToStr(OrderByItem.NullOrdering null_ordering) {
        switch (null_ordering) {
            case NULLS_FIRST:
                return " nulls first";
            case NULLS_LAST:
                return " nulls last";
            default:
                Preconditions.checkArgument(false, "unknown null ordering");
                return null;
        }
    }

    private static class SingletonRefCounts {
        private static Map<String, Integer> refCounts;

        static int getRefCount(String str)
                throws ExpressionException{
            synchronized (SingletonRefCounts.class) {
                // #BAD# копипаст из DistillerUtils
                String upper = str.toUpperCase();
                Integer result = refCounts == null ? null : refCounts.get(upper);
                if (result != null)
                    return result;

                int res = ExprUtils.getUnnamedRefCount(str);
                if (refCounts == null)
                    refCounts = new HashMap<>();
                refCounts.put(upper, res);
                return res;
            }
        }
    }

    protected static int getUnnamedRefCount(String str) throws ExpressionException {
        return SingletonRefCounts.getRefCount(str);
    }

    protected String convertSourceSelect(SourceQuery item, ConvertParams params, ConvertState state)
            throws Exception {
        return '(' + convert(item.findSelect(), params, state) + ')';
    }

    protected String convertSourceTable(SourceTable item, ConvertParams params, ConvertState state)
            throws Exception {
        if (StringUtils.isEmpty(item.table))
            throw new SqlObjectsConvertorException(String.format("%s: не задано имя таблицы", item.getClass().getSimpleName()));
	    return DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(item.table);
    }

    protected String convertTableExpr(TableExpression item, ConvertParams params, ConvertState state)
            throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(convert(item.getSource(), params, state));
        if (!StringUtils.isEmpty(item.alias)) {
            sb.append(' ');
            sb.append(DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(item.alias));
        }
        return sb.toString();
    }

    protected String convertFromItem(FromClauseItem item, ConvertParams params, ConvertState state)
            throws Exception {
        StringBuilder sb = new StringBuilder();
        if (item.getIsJoined()) {
            Join join = item.getJoin();
            if (join == null)
                throw new SqlObjectsConvertorException(String.format("Для подлитого выражения '%s' не определено слияние"));
            sb.append(' ')
            .append(join.joinType.toString())
            .append(' ')
            .append(convert(item.getTableExpr()))
            .append(" ON ")
            .append(convert(join.getJoinOn(), params, state));
        }
        else
            sb.append(convert(item.getTableExpr()));

        return sb.toString();
    }

    protected String convertUnions(UnionsContainer item, ConvertParams params, ConvertState state)
            throws Exception {
        if (!item.isHasChilds())
            return "";
        StringBuilder sb = new StringBuilder();
        for(SqlObject child: item)
            sb.append(convert(child, params, state));
        return sb.toString();
    }

    protected String convertUnionItem(UnionItem item, ConvertParams params, ConvertState state)
            throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(' ');
        sb.append(unionTypeToString(item.unionType).toUpperCase());
        sb.append(' ');
        Select select = item.getSelect();
        UnionsContainer unions = select.findUnions();
        if (unions != null && unions.isHasChilds()) {
            sb.append('(');
            sb.append(convert(select, params, state));
            sb.append(')');
        }
        else
            sb.append(convert(select, params, state));

        return sb.toString();
    }

    protected String convertOrderByItem(OrderByItem item, ConvertParams params, ConvertState state)
            throws Exception {
        StringBuilder sb = new StringBuilder(convert(item.getExpr(), params, state));
        sb.append(' ');
        sb.append(orderDirectionToStr(item.direction));
        if (item.nullOrdering != OrderByItem.NullOrdering.NONE)
            sb.append(nullOrderingToStr(item.nullOrdering));

        return sb.toString();
    }

    protected String convertOrderBy(OrderBy item, ConvertParams params, ConvertState state)
            throws Exception {
        if (!item.isHasChilds())
            return "";
        StringBuilder sb = new StringBuilder(" ORDER BY ");
        boolean isFirst = true;
        for(SqlObject child: item) {
            if (!isFirst)
                sb.append(", ");
            sb.append(convert(child, params, state));
            isFirst = false;
        }
        return sb.toString();
    }

    protected String convertGroupBy(GroupBy item, ConvertParams params, ConvertState state)
            throws Exception {
        TupleExpressions tuple = item.findTuple();
        Preconditions.checkState(tuple != null && tuple.isHasChilds());
        if (tuple == null || !tuple.isHasChilds())
            throw new SqlObjectsConvertorException(String.format("В элементе %s не задан перечень группировок", item.getClass().getSimpleName()));
        Conditions having = item.getHaving();
        StringBuilder sb = new StringBuilder(" GROUP BY ");
        boolean isFirst = true;
        for(SqlObject child: tuple) {
            if (!isFirst)
                sb.append(',');
            sb.append(convert(child, params, state));
            isFirst = false;
        }
        if (having != null && !having.isEmpty()) {
            sb.append(" HAVING ");
            sb.append(convert(having, params, state));
        }
        return sb.toString();
    }

    protected String convertColumn(SelectedColumn item, ConvertParams params, ConvertState state)
            throws Exception {
        ColumnExpression expr = item.getColExpr();
        if (expr == null)
            throw new SqlObjectsConvertorException("В колонке не задано выражение");
        return convert(expr, params, state) +
                        (!StringUtils.isEmpty(item.alias) ? " " + DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(item.alias) : "");
    }

    protected String convertColumns(SelectedColumnsContainer item, ConvertParams params, ConvertState state)
            throws Exception {
        if (!item.isHasChilds())
            throw new SqlObjectsConvertorException("Пустой перечень колонок");

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

    protected String convertCTE(CommonTableExpression item, ConvertParams params, ConvertState state)
            throws Exception{
        boolean isFirst;
        StringBuilder sb = new StringBuilder();
        sb.append(DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(item.alias));
        if (item.columns != null && item.columns.size() != 0) {
            sb.append('(');
            isFirst = true;
            for (String column : item.columns) {
                if (!isFirst)
                    sb.append(',');
                sb.append(DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(column));
                isFirst = false;
            }
            sb.append(')');
        }
        sb.append(" AS (");
        sb.append(convert(item.findSelect(), params, state));
        sb.append(')');
        CTECycleClause cycle = item.getCycle();
        if (cycle != null) {
            sb.append("CYCLE ");
            isFirst = true;
            for (String column: cycle.columns) {
                if (!isFirst)
                    sb.append(',');
                sb.append(DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(column));
                isFirst = false;
            }
            sb.append(" SET ");
            sb.append(cycle.markerCycleColumn);
            sb.append(" TO '");
            sb.append(cycle.markerCycleValue);
            sb.append("' DEFAULT '");
            sb.append(cycle.markerCycleValueDefault);
            sb.append('\'');
            if (!StringUtils.isEmpty(cycle.columnPath))
                sb.append(" USING " + cycle.columnPath);
        }
        return sb.toString();
    }

    protected String convertWith(CTEsContainer item, ConvertParams params, ConvertState state)
            throws Exception{
        if (!item.isHasChilds())
            return "";

        StringBuilder sb = new StringBuilder("WITH ");
        boolean isFirst = true;
        for (SqlObject child: item) {
            if (!isFirst)
                sb.append(',');
            sb.append(convert(child, params, state));
            isFirst = false;
        }
        return sb.toString();
    }

    protected String convertFrom(FromContainer item, ConvertParams params, ConvertState state)
            throws Exception{
        if (!item.isHasChilds())
            return "";

        StringBuilder sb = new StringBuilder(" FROM ");
        for (SqlObject child: item)
            sb.append(convert(child, params, state));

        return sb.toString();
    }

    protected String convertCall(CallStoredProcedure item, ConvertParams params, ConvertState state)
            throws Exception{
        StringBuilder sb = new StringBuilder("CALL ");
        sb.append(item.spName);

        TupleExpressions tuple = item.findTuple();
        if (tuple != null && tuple.isHasChilds()) {
            sb.append('(');
            boolean isFirst = true;
            for (SqlObject child: tuple) {
                if (!isFirst)
                    sb.append(',');
                sb.append(convert(child, params, state));
                isFirst = false;
            }
            sb.append(')');
        }
        return sb.toString();
    }

    protected String returningAsString(TupleExpressions returning, ConvertParams params, ConvertState state)
            throws Exception{
        if (returning == null)
            return "";

        StringBuilder sb = new StringBuilder();
        for (SqlObject item: returning) {
            if (sb.length() != 0)
                sb.append(',');
            sb.append(convert(item, params, state));
        }
        return sb.toString();
    }

    protected String convertDelete(SqlQueryDelete item, ConvertParams params, ConvertState state)
            throws Exception{
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        // TODO хинт
        sb.append(item.table);

        Conditions where = item.findWhere();
        if (where != null && !where.isEmpty()) {
            sb.append(" WHERE ");
            sb.append(convert(where, params, state));
        }

        TupleExpressions returning = item.getReturning();
        if (returning != null && returning.isHasChilds()) {
            sb.append(" RETURNING ");
            sb.append(returningAsString(returning, params, state));
        }

        return sb.toString();
    }

    protected String convertUpdate(SqlQueryUpdate item, ConvertParams params, ConvertState state)
            throws Exception{
        boolean isFirst;
        StringBuilder sb = new StringBuilder("UPDATE ");
        // TODO хинт
        sb.append(item.table);

        DMLFieldsAssignments assignments = item.getAssignments();
        if (assignments.isHasChilds()) {
            sb.append(" SET ");
            isFirst = true;
            for (SqlObject assignment : assignments) {
                if (!isFirst)
                    sb.append(',');
                sb.append(convert(((DMLFieldAssignment)assignment).getField(), params, state));
                sb.append('=');
                sb.append(convert(((DMLFieldAssignment)assignment).getExpr(), params, state));
                isFirst = false;
            }
        }

        Conditions where = item.findWhere();
        if (where != null && !where.isEmpty()) {
            sb.append(" WHERE ");
            sb.append(convert(where, params, state));
        }

        TupleExpressions returning = item.getReturning();
        if (returning != null && returning.isHasChilds()) {
            sb.append(" RETURNING ");
            sb.append(returningAsString(returning, params, state));
        }

        return sb.toString();
    }

    protected String convertInsert(SqlQueryInsert item, ConvertParams params, ConvertState state)
            throws Exception{
        boolean isFirst;
	    StringBuilder sb = new StringBuilder("INSERT ");
        // TODO хинт
        sb.append("INTO ");
        sb.append(item.table);

        DMLFieldsAssignments assignments = item.getAssignments();
        if (assignments.isHasChilds()) {
            sb.append('(');
            isFirst = true;
            for (SqlObject assignment : assignments) {
                if (!isFirst)
                    sb.append(',');
                sb.append(convert(((DMLFieldAssignment)assignment).getField(), params, state));
                isFirst = false;
            }
            sb.append(')');
        }
        Select select = item.findSelect();
        if (select == null) {
            sb.append(" VALUES(");
            isFirst = true;
            for (SqlObject assignment : assignments) {
                if (!isFirst)
                    sb.append(',');
                sb.append(convert(((DMLFieldAssignment)assignment).getExpr(), params, state));
                isFirst = false;
            }
            sb.append(')');
        }
        else {
            sb.append(" (");
            sb.append(convert(select, params, state));
            sb.append(')');
        }

        TupleExpressions returning = item.getReturning();
        if (returning != null && returning.isHasChilds()) {
            sb.append(" RETURNING ");
            sb.append(returningAsString(returning, params, state));
        }
        return sb.toString();
    }

    protected String convertFromClause(FromContainer from, ConvertParams params, ConvertState state)
            throws Exception{
        if (from == null || !from.isHasChilds())
            return "";
        return convert(from, params, state);
    }

    protected String convertWhereClause(Conditions where, ConvertParams params, ConvertState state)
            throws Exception{
        if (where == null || where.isEmpty())
            return "";
        return " WHERE " + convert(where, params, state);
    }

    protected String convertSelect(Select item, ConvertParams params, ConvertState state)
            throws Exception{
        CTEsContainer with = item.findWith();
        SelectedColumnsContainer columns = item.getColumns();
        FromContainer from = item.findFrom();
        Conditions where = item.findWhere();
        GroupBy groupBy = item.findGroupBy();
        UnionsContainer unions = item.findUnions();

        StringBuilder sb = new StringBuilder();
        
        if (with != null)
            sb.append(convert(with, params, state));
        
        sb.append("SELECT ");
        // TODO хинт
        if (item.distinct)
            sb.append("DISTINCT ");
        sb.append(convert(columns, params, state));

        sb.append(convertFromClause(from, params, state));

        sb.append(convertWhereClause(where, params, state));

        if (groupBy != null)
            sb.append(convert(groupBy, params, state));

        if (unions != null)
            sb.append(convert(unions, params, state));

        return sb.toString();
    }

    protected String convertCursorSpec(CursorSpecification item, ConvertParams params, ConvertState state)
            throws Exception{
        String strSelect = convert(item.findSelect(), params, state);
        OrderBy orderBy = item.findOrderBy();
        String strOrderBy = orderBy != null ? convert(orderBy, params, state) : "";
        return strSelect + strOrderBy + (item.getFetchOffset() > 0 ? " OFFSET " + item.getFetchOffset() : "") + (item.getFetchFirst() > 0 ? " LIMIT " + item.getFetchFirst() : "");
    }

    protected boolean isNegPredicateSql(String sql) {
        Preconditions.checkArgument(!StringUtils.isEmpty(sql));
        return (sql.startsWith("NOT ") || sql.startsWith("NOT(") || sql.startsWith("NOT/*") || sql.startsWith("NOT--"));
    }

    protected String convertPredicateBracketWithOnePredicate(Conditions item, ConvertParams params, ConvertState state)
            throws Exception {
        Predicate oncePredicate = (Predicate)item.firstSubItem();
        String sqlPredicate = convert(oncePredicate, params, state);
        if (StringUtils.isEmpty(sqlPredicate))
            throw new SqlObjectsConvertorException("условие сконвертировалось в пустой sql");
        if (!item.not)
            return sqlPredicate;
        else if (isNegPredicateSql(sqlPredicate))
            return "NOT(" + sqlPredicate + ')';
        else
            return "NOT " + sqlPredicate;
    }

    protected String convertPredicateBracketWithManyPredicates(Conditions item, ConvertParams params, ConvertState state)
            throws Exception {
        boolean ownedByConditions = item.getOwner() != null && item.getOwner().getClass() == Conditions.class;
        boolean needWrapInBrackets =
                item.not ||
                        (ownedByConditions && ((Conditions)item.getOwner()).booleanOp != item.booleanOp);

        StringBuilder sb = new StringBuilder();

        if (item.not)
            if (needWrapInBrackets)
                sb.append("NOT(");
            else
                sb.append("NOT ");
        else if (needWrapInBrackets)
            sb.append('(');

        String booleanOp;
        switch (item.booleanOp) {
            case AND:
                booleanOp = " AND ";
                break;
            case OR:
                booleanOp = " OR ";
                break;
            default:
                throw new SqlObjectsConvertorException("Неизвестная логическая операция " + item.booleanOp);
        }
        boolean isFirst = true;
        for(SqlObject child: item) {
            if (!isFirst)
                sb.append(booleanOp);
            String sqlPredicate = convert(child, params, state);
            if (isNegPredicateSql(sqlPredicate) && item.not) {
                sb.append('(');
                sb.append(sqlPredicate);
                sb.append(')');
            }
            else
                sb.append(sqlPredicate);
            isFirst = false;
        }

        if (needWrapInBrackets)
            sb.append(')');

        return sb.toString();
    }

    protected String convertPredicateBracket(Conditions item, ConvertParams params, ConvertState state)
            throws Exception {

        if (item.isEmpty())
            throw new SqlObjectsConvertorException("Пустой набор условий");

        if (item.itemsCount() == 1)
            return convertPredicateBracketWithOnePredicate(item, params, state);
        else
            return convertPredicateBracketWithManyPredicates(item, params, state);
    }

    protected String convertPredicateIsNull(PredicateIsNull item, ConvertParams params, ConvertState state)
            throws Exception{
        return convert(item.getExpr(), params, state) + " IS" +  (item.not ? " NOT" : "") + " NULL";
    }

    protected String convertPredicateInSelect(PredicateInQuery item, ConvertParams params, ConvertState state)
            throws Exception{
        StringBuilder result = new StringBuilder("(");
        boolean first = true;
        for (SqlObject child: item.getTuple()) {
            if (first)
                first = false;
            else
                result.append(',');

            result.append(convert(child, params, state));
        }
        result.append(")" + (item.not ? " NOT" : "") + " IN (");
        result.append(convert(item.findSelect(), params, state));
        result.append(')');
        return result.toString();
    }

    protected String convertPredicateInTuple(PredicateInTuple item, ConvertParams params, ConvertState state)
            throws Exception{
        StringBuilder result = new StringBuilder(convert(item.getExpr(), params, state));
        result.append((item.not ? " NOT" : "") + " IN (");
        boolean first = true;
        for (SqlObject child : item.getTuple()) {
            if (first) {
                first = false;
            }
            else {
                result.append(",");
            }
            result.append(convert(child, params, state));
        }
        result.append(")");
        return result.toString();
    }

    protected String convertPredicateLike(PredicateLike item, ConvertParams params, ConvertState state)
            throws Exception{
        StringBuilder result = new StringBuilder(
                convert(item.getLeft(), params, state));
        result.append((item.not ? " NOT" : "") + " LIKE ");
        result.append(convert(item.getRight(), params, state));
        if (!StringUtils.isEmpty(item.escape)) {
            result.append(" ESCAPE ");
            result.append("'");
            result.append(item.escape.replaceAll("'", "''"));
            result.append("'");
        }
        return result.toString();
    }

    protected String convertPredicateExists(PredicateExists item, ConvertParams params, ConvertState state)
            throws Exception{
        return (item.not ? "NOT " : "") + "EXISTS(" + convert(item.findSelect(), params, state) + ")";
    }

    protected String convertPredicateBetween(PredicateBetween item, ConvertParams params, ConvertState state)
            throws Exception{
        return convert(item.getExpr(), params, state) +
                (item.not ? " NOT" : "") +
                " BETWEEN " +
                convert(item.getLeft(), params, state) +
                " AND " +
                convert(item.getRight(), params, state);
    }

    protected String convertPredicateComparison(PredicateComparison item, ConvertParams params, ConvertState state)
            throws Exception{
        return (item.not ? "NOT " : "") + convert(item.getLeft(), params, state) +
                item.comparison.toString() +
                convert(item.getRight(), params, state);
    }

    protected String convertValueConst(ValueConst item, ConvertParams params, ConvertState state)
            throws Exception{
        return getDBSupport().expressionStrForValue(item);
    }

    protected String convertScalar(Scalar item, ConvertParams params, ConvertState state)
            throws Exception{
        return "(" + convert(item.findSelect(), params, state) + ")";
    }

    protected String convertParamRef(ParamRef item, ConvertParams params, ConvertState state)
            throws Exception{
        if (params.useJDbcParamRef())
            return "?";

        if (StringUtils.isEmpty(item.parameterName))
            throw new SqlObjectsConvertorException("Не указано имя параметра");
        return ":" + DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(item.parameterName);
    }

    protected String convertCaseSearch(CaseSearch item, ConvertParams params, ConvertState state)
            throws Exception{
        StringBuilder result = new StringBuilder("(CASE ");
        for (SqlObject child: item)
            if (child.getClass() == CaseSearch.WhenThen.class) {
                CaseSearch.WhenThen searchCase =
                        (CaseSearch.WhenThen) child;
                result.append("WHEN " +
                        convert(searchCase.getWhen(), params, state) +
                        " THEN " + convert(searchCase.getThen(), params, state) + " ");
            }

        ColumnExpression elseExpr = item.getElse();
        if (elseExpr != null) {
            result.append("ELSE " + convert(elseExpr, params, state) + " ");
        }
        result.append("END)");
        return result.toString();
    }

    protected String convertCaseSimple(CaseSimple item, ConvertParams params, ConvertState state)
            throws Exception{
        // TODO при конвертации в sql конструкция case оборачивается в скобки: "(CASE...END)". Наверное это необязательно.
        // но если скобки не ставить - нужно будет править тесты
        StringBuilder result = new StringBuilder("(CASE "+ convert(item.getCase(), params, state) + " ");
        for (SqlObject child: item)
            if (child.getClass() == CaseSimple.WhenThen.class) {
                CaseSimple.WhenThen simpleCase = (CaseSimple.WhenThen) child;
                result.append("WHEN " +
                        convert(simpleCase.getWhen(), params, state) +
                        " THEN " + convert(simpleCase.getThen(), params, state) + " ");
            }
        ColumnExpression elseExpr = item.getElse();
        if (elseExpr != null)
            result.append("ELSE " + convert(elseExpr, params, state) + " ");

        result.append("END)");
        return result.toString();
    }

    protected String convertExpression(Expression item, ConvertParams params, ConvertState state)
            throws Exception {
        if (!item.isPureSql)
            throw new SqlObjectsConvertorException(String.format("Выражение не готово к конвертации в sql"));
        String expr = item.getExpr();
        if (StringUtils.isEmpty(expr))
            throw new SqlObjectsConvertorException(String.format("Строка выражения не задана"));
        if (getUnnamedRefCount(expr) != item.itemsCount())
            throw new ExpressionException(String.format(ExprUtils.NOT_CORRESPONDING_UNNAMED_ARG_REFS_WITH_EXPR_CHILDS, expr));
        if (!item.isHasChilds())
            return expr;

        int unnamedRefIndex = 0;
        int posCurrent = 0;
        int posBuildFrom = posCurrent;
        boolean isLastPartBuilded = false;
        StringBuilder sb = new StringBuilder();
        while (posCurrent < expr.length()) {
            switch (expr.charAt(posCurrent)) {
                case '\'':
                    posCurrent = ExprUtils.getPosAfterLiteral(expr, posCurrent);
                    isLastPartBuilded = false;
                    break;
                case Expression.CHAR_BEFORE_TOKEN:
                    if (!expr.substring(posCurrent, posCurrent + Expression.UNNAMED_ARGUMENT_REF.length())
                            .equals(Expression.UNNAMED_ARGUMENT_REF))
                        throw new SqlObjectsConvertorException(String.format("В выражении '%s' ожидается безымянная ссылка на аргумент", expr));
                    sb.append(expr, posBuildFrom, posCurrent);
                    posCurrent += Expression.UNNAMED_ARGUMENT_REF.length();
                    posBuildFrom = posCurrent;
                    sb.append(convert(item.getItem(unnamedRefIndex), params, state));
                    unnamedRefIndex++;
                    isLastPartBuilded = true;
                    break;
                default:
                    posCurrent++;
                    isLastPartBuilded = false;
            }
        }
        if (!isLastPartBuilded)
            sb.append(expr, posBuildFrom, posCurrent);
        return sb.toString();
    }

    protected String convertQField(QualifiedField item, ConvertParams params, ConvertState state)
            throws SqlObjectsConvertorException{
        if (SqlObjectUtils.isAsterisk(item))
            throw new SqlObjectsConvertorException("Конвертация разворачиваемой звёздочки не поддержана");
        if (SqlObjectUtils.isRecordId(item))
            throw new SqlObjectsConvertorException("Конвертация RecordId не поддержана");
        String result = item.fieldName;
        if (!StringUtils.isEmpty(item.alias))
            return DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(item.alias) + "." + DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(result);
        return DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(result);
    }

}
