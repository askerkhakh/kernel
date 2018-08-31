package ru.sonarplus.kernel.sqlobject.db_support_ora;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsConvertor;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsDbSupportUtils;
import ru.sonarplus.kernel.sqlobject.objects.*;

public class SqlObjectsConvertorOra extends SqlObjectsConvertor {

    public SqlObjectsConvertorOra(boolean useStandardNulls) { super(useStandardNulls); }

    public SqlObjectsConvertorOra() { super(true); }

    public static class ConvertParamsOra extends ConvertParams {
        public ConvertParamsOra() { super(); }

        public boolean useOraJoinsForClause(FromContainer from) {
            // TODO потенциальная возможность использовать не-Oracle, Ansi-слияния для отдельно взятого раздела FROM
            return true;
        }

        // ограничения на количество записей с использованием ROWNUM
        // когда true - используется синтаксис с ROWNUM,
        // в случае false - другой синтаксис (в данный момент - ошибка)
        public final boolean useFetchStatementWithRowNum = true;
    }

    public static class ConvertStateOra extends ConvertState {

        public static class JoinContext {
            public String joinedTable;
            public Join.JoinType joinType;

            public JoinContext(String joinedTable, Join.JoinType joinType) {
                this.joinedTable = joinedTable;
                this.joinType = joinType;
            }
        }
        private StringBuilder sbJoinConditions = null;

        public boolean needUseOraJoins = false;
        public JoinContext joinContext = null;

        public ConvertStateOra() { super(); }

        public String getJoinConditions() {
            if (this.sbJoinConditions == null || this.sbJoinConditions.length() == 0)
                return null;
            else
                return this.sbJoinConditions.toString();
        }

        public void setJoinConditions(String value) {
            if (this.sbJoinConditions == null)
                this.sbJoinConditions = StringUtils.isEmpty(value) ? new StringBuilder() : new StringBuilder(value);
            else {
                this.sbJoinConditions.setLength(0);
                if (!StringUtils.isEmpty(value))
                    this.sbJoinConditions.append(value);
            }
        }
        public void addJoinPredicate(String joinPredicate) {
            if (StringUtils.isEmpty(joinPredicate))
                return;
            if (this.sbJoinConditions == null)
                this.sbJoinConditions = new StringBuilder(joinPredicate);
            else {
                if (this.sbJoinConditions.length() != 0)
                    this.sbJoinConditions.append(" AND ");
                this.sbJoinConditions.append(joinPredicate);
            }
        }

        public long fetchFirst = 0;

        public ConvertStateOra setFetchFirst(long value) {
            this.fetchFirst = value > 0 ? value : 0;
            return this;
        }
    }

    @Override
    protected SqlObjectsDbSupportUtils createDBSupport() { return new SqlObjectsDbSupportUtilsOra(); }

    @Override
    protected void setupConvertParams(ConvertParams params) {}

    @Override
    protected Class getConvertParamsClass() { return ConvertParamsOra.class; }
    @Override
    protected Class getConvertStateClass() { return ConvertStateOra.class; }

    @Override
    protected String internalConvertItem(SqlObject item, ConvertParams params, ConvertState state) throws Exception {

        Preconditions.checkNotNull(item);

        if (item != null && item.getClass() == PredicateRegExpMatch.class)
            return convertPredicateRegExp((PredicateRegExpMatch) item, params, state);
        else
            return super.internalConvertItem(item, params, state);
    }

    @Override
    public String unionTypeToString(UnionItem.UnionType unionType) {
        switch (unionType) {
            case MINUS:
                return "MINUS";
            default:
                return super.unionTypeToString(unionType);
        }
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

    protected String convertFromItem(FromClauseItem item, ConvertParams params, ConvertState state)
            throws Exception {
        if (item.getIsJoined()) {
            Join join = item.getJoin();
            if (join == null)
                return convert(item.getTableExpr());
            else {
                ConvertStateOra stateOra = (ConvertStateOra) state;
                if (stateOra.needUseOraJoins) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(convert(item.getTableExpr()));
                    stateOra.joinContext = new ConvertStateOra.JoinContext(item.getAliasOrName(), join.joinType);
                    try {
                        stateOra.addJoinPredicate(convert(join.getJoinOn(), params, state));
                        return sb.toString();
                    } finally {
                        stateOra.joinContext = null;
                    }
                } else
                    return super.convertFromItem(item, params, state);
            }
        }
        else
            return convert(item.getTableExpr());
    }

    protected String convertFrom(FromContainer item, ConvertParams params, ConvertState state)
            throws Exception{
	    ConvertStateOra stateOra = (ConvertStateOra)state;
        StringBuilder sb = new StringBuilder(" FROM ");
        boolean isFirst = true;
        for (SqlObject child : item) {
            if (!isFirst && stateOra.needUseOraJoins)
                sb.append(',');
            sb.append(convert(child, params, state));
            isFirst = false;
        }
        return sb.toString();
    }

    @Override
    protected String convertFromClause(FromContainer from, ConvertParams params, ConvertState state)
            throws Exception{
        if (from == null || !from.isHasChilds())
            return " FROM DUAL";
        return super.convertFromClause(from, params, state);
    }

    @Override
    protected String convertWhereClause(Conditions where, ConvertParams params, ConvertState state)
            throws Exception{
        boolean hasWhere = where != null && !where.isEmpty();
        ConvertStateOra stateOra = (ConvertStateOra) state;
        String joinConditions = stateOra.getJoinConditions();
        boolean hasJoins = !StringUtils.isEmpty(joinConditions);
        if (stateOra.fetchFirst > 0 || hasJoins || hasWhere) {
            StringBuilder sb = new StringBuilder();
            sb.append(" WHERE ");
            boolean addedConditions = false;
            if (stateOra.fetchFirst > 0) {
                Preconditions.checkState(((ConvertParamsOra)params).useFetchStatementWithRowNum);
                if (addedConditions)
                    sb.append(" AND ");
                sb.append("ROWNUM <= ");
                sb.append(stateOra.fetchFirst);
                addedConditions = true;
            }
            if (hasJoins) {
                if (addedConditions)
                    sb.append(" AND ");
                sb.append(joinConditions);
                addedConditions = true;
            }
            if (hasWhere) {
                if (addedConditions)
                    sb.append(" AND ");
                boolean needBrackets = addedConditions && (where.booleanOp != Conditions.BooleanOp.AND || where.not);
                if (needBrackets)
                    sb.append('(');
                sb.append(convert(where, params, state));
                if (needBrackets)
                    sb.append(')');
            }
            return sb.toString();
        }
        return "";
    }

    @Override
    protected String convertColumns(SelectedColumnsContainer item, ConvertParams params, ConvertState state)
            throws Exception{
        String result = super.convertColumns(item, params, state);
        ConvertStateOra stateOra = (ConvertStateOra) state;
        if (stateOra.fetchFirst > 0)
            return new StringBuilder()
                    //.append("/*+ FIRST_ROWS(").append(stateOra.fetchFirst).append(") */")
                    .append(result).toString();
        else
            return result;
    }

    protected String convertSelect(Select item, ConvertParams params, ConvertState state)
            throws Exception{
        ConvertStateOra stateOra = (ConvertStateOra)state;
        boolean backUseOraJoins = stateOra.needUseOraJoins;
        stateOra.needUseOraJoins = ((ConvertParamsOra)params).useOraJoinsForClause(item.findFrom());
        String backJoinConditions = stateOra.getJoinConditions();
        stateOra.setJoinConditions(null);
        ConvertStateOra.JoinContext backJoinContext = stateOra.joinContext;
        stateOra.joinContext = null;
        Long backFetchFirst = null;
        if (!(item.getOwner() instanceof CursorSpecification)) {
            // в данный момент ограничение на количество записей реализуется с помощью добавления
            // в блок WHERE условия по ROWNUM.
            // чтобы не добавлять ROWNUM во все подзапросы, для Select-ов, не содержащихся в CursorSpec'е
            // перед их конвертацией fetchFirst сбрасываем...
            backFetchFirst = stateOra.fetchFirst;
            stateOra.fetchFirst = 0;
        }
        try {
            return super.convertSelect(item, params, state);
        }
        finally {
            stateOra.setJoinConditions(backJoinConditions);
            stateOra.joinContext = backJoinContext;
            stateOra.needUseOraJoins = backUseOraJoins;
            // ...а потом восстанавливаем
            if (backFetchFirst != null)
                stateOra.setFetchFirst(backFetchFirst);
        }
    }

    // как public, чтобы иметь возможность использовать константу в тестах
    public final static String UNIQUE_COLUMN_PREFIX = "C_9LBW2CZUP_";
    private void ensureUniqueColumnsAliases(SelectedColumnsContainer columns) {
        // при назначении колонкам уникальных алиасов не проверяем,
        // является ли колонка не выражением, а звёздочкой '*'
        // (в этом случае неквалифицированная звёздочка должна быть единственной колонкой,
        // а звёздочка вообще - не должна иметь алиас)
        // в Delphi также не проверяем, но может быть здесь это будет нужно?
        int i = 0;
        for (SqlObject item: columns) {
            SelectedColumn column = (SelectedColumn) item;
            if (StringUtils.isEmpty(column.alias)) {
                column.alias = UNIQUE_COLUMN_PREFIX + i++;
            }
        }
    }

    protected String convertCursorSpecOrderedSelect(CursorSpecification item, ConvertParams params, ConvertState state)
            throws Exception{
        return new StringBuilder()
                .append(convert(item.getSelect(), params, state))
                .append(convert(item.getOrderBy(), params, state))
                .toString();
    }

    protected String convertCursorSpecSelectOffset(CursorSpecification item, ConvertParams params, ConvertState state)
            throws Exception{
        Select select = item.getSelect();
        SelectedColumnsContainer columns = select.getColumns();
        SelectedColumnsContainer orgColumns = (SelectedColumnsContainer) columns.clone();
        try {
            ensureUniqueColumnsAliases(columns);
            return appendColumns(select, new StringBuilder("SELECT "))
                    .append(" FROM(SELECT a.*, ROWNUM rnum FROM(")
                    .append(convert(item.getSelect(), params, state))
                    .append(") a) WHERE rnum >").append(item.getFetchOffset())
                    .toString();
        } finally {
            select.setColumns(orgColumns);
        }

    }

    protected String convertCursorSpecOrderedSelectOffset(CursorSpecification item, ConvertParams params, ConvertState state)
            throws Exception{
        Select select = item.getSelect();
        SelectedColumnsContainer columns = select.getColumns();
        SelectedColumnsContainer orgColumns = (SelectedColumnsContainer) columns.clone();
        try {
            ensureUniqueColumnsAliases(columns);
            return appendColumns(select, new StringBuilder("SELECT "))
                    .append(" FROM(SELECT a.*, ROWNUM rnum FROM(")
                    .append(convert(item.getSelect(), params, state))
                    .append(convert(item.getOrderBy(), params, state))
                    .append(") a) WHERE rnum >").append(item.getFetchOffset())
                    .toString();
        } finally {
            select.setColumns(orgColumns);
        }
    }

    protected String convertCursorSpecSelectFirst(CursorSpecification item, ConvertParams params, ConvertState state)
            throws Exception{
        ((ConvertStateOra) state).setFetchFirst(item.getFetchFirst());
        return convert(item.getSelect(), params, state);
    }

    protected String convertCursorSpecOrderedSelectFirst(CursorSpecification item, ConvertParams params, ConvertState state)
            throws Exception{
        Select select = item.getSelect();
        SelectedColumnsContainer columns = select.getColumns();
        SelectedColumnsContainer orgColumns = (SelectedColumnsContainer) columns.clone();
        try {
            ensureUniqueColumnsAliases(columns);
            return appendColumns(select, new StringBuilder("SELECT ")
                    //.append("/*+ FIRST_ROWS(").append(item.fetchFirst).append(") */ ")
            )
                    .append(" FROM(")
                    .append(convert(item.getSelect(), params, state))
                    .append(convert(item.getOrderBy(), params, state))
                    .append(") WHERE ROWNUM <= ").append(item.getFetchFirst())
                    .toString();
        } finally {
            select.setColumns(orgColumns);
        }
    }

    protected String convertCursorSpecSelectOffsetFirst(CursorSpecification item, ConvertParams params, ConvertState state)
            throws Exception{
        Select select = item.getSelect();
        SelectedColumnsContainer columns = select.getColumns();
        SelectedColumnsContainer orgColumns = (SelectedColumnsContainer) columns.clone();
        try {
            ensureUniqueColumnsAliases(columns);
            ((ConvertStateOra) state).setFetchFirst(item.getFetchOffset() + item.getFetchFirst());
            return appendColumns(select, new StringBuilder("SELECT "))
                    .append(" FROM (")
                    .append("SELECT a.*, ROWNUM rnum FROM(")
                    .append(convert(item.getSelect(), params, state)).append(") a")
                    .append(") WHERE rnum >").append(item.getFetchOffset())
                    .toString();
        } finally {
            select.setColumns(orgColumns);
        }
    }

    protected StringBuilder appendColumns(Select select, StringBuilder sb) {
        boolean isFirst = true;
        for (SqlObject child: select.getColumns()) {
            if (!isFirst)
                sb.append(", ");
            sb.append(((SelectedColumn)child).alias);
            isFirst = false;
        }
        return sb;
    }

    protected String convertCursorSpecOrderedSelectOffsetFirst(CursorSpecification item, ConvertParams params, ConvertState state)
            throws Exception{
        Select select = item.getSelect();
        SelectedColumnsContainer columns = select.getColumns();
        SelectedColumnsContainer orgColumns = (SelectedColumnsContainer) columns.clone();
        try {
            ensureUniqueColumnsAliases(columns);
            return appendColumns(select, new StringBuilder("SELECT "))
                    .append(" FROM (")
                    .append("SELECT ")
                    //.append("/*+ FIRST_ROWS(").append(item.fetchOffset + item.fetchFirst).append(") */ ")
                    .append("a.*, ROWNUM rnum FROM(")
                    .append(convert(item.getSelect(), params, state))
                    .append(convert(item.getOrderBy(), params, state))
                    .append(") a WHERE ROWNUM <= ").append(item.getFetchOffset() + item.getFetchFirst())
                    .append(") WHERE rnum >").append(item.getFetchOffset())
                    .toString();
        } finally {
            select.setColumns(orgColumns);
        }
    }

    @Override
    protected String convertCursorSpec(CursorSpecification item, ConvertParams params, ConvertState state)
            throws Exception{
        final byte CS_SELECT                      = (byte)0b0000000;
        final byte CS_SELECT_ORDERED              = (byte)0b0000001;
        final byte CS_SELECT_FIRST                = (byte)0b0000010;
        final byte CS_SELECT_OFFSET               = (byte)0b0000100;

        final byte CS_SELECT_OFFSET_FIRST         = (byte)0b0000110;
        final byte CS_SELECT_ORDERED_FIRST        = (byte)0b0000011;
        final byte CS_SELECT_ORDERED_OFFSET       = (byte)0b0000101;
        final byte CS_SELECT_ORDERED_OFFSET_FIRST = (byte)0b0000111;

        byte cs_state = CS_SELECT;
        if (item.findOrderBy() != null)
            cs_state |= CS_SELECT_ORDERED;
        if (item.getFetchOffset() > 0)
            cs_state |= CS_SELECT_OFFSET;
        if (item.getFetchFirst() > 0)
            cs_state |= CS_SELECT_FIRST;

        if (cs_state == CS_SELECT_FIRST ||
                cs_state == CS_SELECT_OFFSET ||
                cs_state == CS_SELECT_OFFSET_FIRST ||
                cs_state == CS_SELECT_ORDERED_FIRST ||
                cs_state == CS_SELECT_ORDERED_OFFSET ||
                cs_state == CS_SELECT_ORDERED_OFFSET_FIRST)
            Preconditions.checkState(((ConvertParamsOra)params).useFetchStatementWithRowNum);

        switch (cs_state) {
            case CS_SELECT:
                return convert(item.getSelect(), params, state);
            case CS_SELECT_ORDERED:
                return convertCursorSpecOrderedSelect(item, params, state);
            case CS_SELECT_FIRST:
                return convertCursorSpecSelectFirst(item, params, state);
            case CS_SELECT_OFFSET:
                return convertCursorSpecSelectOffset(item, params, state);
            case CS_SELECT_OFFSET_FIRST:
                return convertCursorSpecSelectOffsetFirst(item, params, state);
            case CS_SELECT_ORDERED_FIRST:
                return convertCursorSpecOrderedSelectFirst(item, params, state);
            case CS_SELECT_ORDERED_OFFSET:
                return convertCursorSpecOrderedSelectOffset(item, params, state);
            case CS_SELECT_ORDERED_OFFSET_FIRST:
                return convertCursorSpecOrderedSelectOffsetFirst(item, params, state);
            default:
                throw new SqlObjectsConvertorException("cursor spec order by offset fetch first...");
        }
    }

    @Override
    protected String convertValueConst(ValueConst item, ConvertParams params, ConvertState state)
            throws Exception{
        return getDBSupport().expressionStrForValue(item);
    }

    protected boolean containedInComparison(ColumnExpression item) {
        if (
                item.getOwner() != null && (item.getOwner().getClass() == PredicateComparison.class
                        // нотация слияний Oracle позволяет использовать '(+)' и в условиях '...is [not] null'
                        || item.getOwner().getClass() == PredicateIsNull.class
                ))
            return true;
        if (item.getOwner() instanceof ColumnExpression)
            return containedInComparison((ColumnExpression) item.getOwner());
        return false;
    }

    @Override
    protected String convertQField(QualifiedField item, ConvertParams params, ConvertState state)
            throws SqlObjectsConvertorException{
        String result = super.convertQField(item, params, state);
        ConvertStateOra stateOra = (ConvertStateOra)state;
        if (stateOra.joinContext != null &&
                stateOra.needUseOraJoins &&
                containedInComparison(item)
                ) {
            if (stateOra.joinContext.joinType == Join.JoinType.LEFT) {
                if (!StringUtils.isEmpty(item.alias) && item.alias.equals(stateOra.joinContext.joinedTable))
                    result = result + "(+)";
            }
            else if (stateOra.joinContext.joinType == Join.JoinType.RIGHT) {
                if (StringUtils.isEmpty(item.alias) || !item.alias.equals(stateOra.joinContext.joinedTable))
                    result = result + "(+)";
            }
            else if (stateOra.joinContext.joinType == Join.JoinType.INNER) {
                //
            }
            else
                Preconditions.checkState(false, stateOra.joinContext.joinType);
        }
        return result;
    }

}
