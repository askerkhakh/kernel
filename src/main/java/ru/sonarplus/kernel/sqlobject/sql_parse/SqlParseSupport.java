package ru.sonarplus.kernel.sqlobject.sql_parse;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.CodeValue;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;
import ru.sonarplus.kernel.sqlobject.expressions.Expr;
import ru.sonarplus.kernel.sqlobject.objects.*;

import java.util.ArrayList;
import java.util.List;

public class SqlParseSupport {

    public static String dequoteIdentifier(String value) {
        // идентификатор может быть взят в "".
        // если попали в эту функцию - значит парсер уже справился с разбором идентификатора
        // и value здесь  либо точно содержит "" в начале и конце либо точно нет.
        if (value.charAt(0)=='"')
            return value.substring(1, value.length() - 1);
        else
            return value;
    }

    public static String dequoteLiteralString(String value) {
        // аналогично - раз уж попали в этот обработчик, значит строковый литерал уже разобран
        // и представлен в виде "'...'". уберём одинарные кавычки
        return value.substring(1, value.length() - 1);
    }

    public static CallStoredProcedure buildCallSP(List<String> identProc, Object tuple)
            throws SqlObjectException {
        CallStoredProcedure request = new CallStoredProcedure();
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String s: identProc) {
            if (!first)
                sb.append('.');
            sb.append(s);
            first = false;
        }
        request.spName = sb.toString();
        if (tuple != null)
            //TODO Определиться - нужно ли здесь проверять среди аргументов вызова хранимой функции наличие имён полей
            request.setTuple((TupleExpressions) tuple);
        return request;
    }

    public static DMLFieldAssignment buildAssignment(String ident, Object expr, boolean backQuoted)
            throws SqlObjectException {
        DMLFieldAssignment assignment = new DMLFieldAssignment();
        if (backQuoted)
            assignment.setField(new QualifiedField("", ident.substring(1)));
        else
            assignment.setField(new QualifiedRField("", ident));
        assignment.setExpr((ColumnExpression) Preconditions.checkNotNull(expr));
        return assignment;
    }

    public static DMLFieldsAssignments buildAssignments(Object assignment)
            throws SqlObjectException {
        DMLFieldsAssignments assignments = new DMLFieldsAssignments();
        assignments.insertItem((DMLFieldAssignment)assignment);
        return assignments;
    }

    public static DMLFieldsAssignments buildAssignments(Object assignments, Object assignment)
            throws SqlObjectException {
        DMLFieldsAssignments fieldAssignments = (DMLFieldsAssignments) assignments;
        fieldAssignments.insertItem((DMLFieldAssignment)assignment);
        return fieldAssignments;
    }

    public static SqlQueryUpdate buildUpdate(String identTable, Object clauseSet, Object clauseWhere)
            throws SqlObjectException {
        SqlQueryUpdate request = new SqlQueryUpdate();
        request.table = identTable;
        request.insertItem((SqlObject) Preconditions.checkNotNull(clauseSet));
        if (clauseWhere != null)
            request.insertItem((SqlObject) clauseWhere);
        return request;
    }

    public static SqlQueryDelete buildDelete(String identTable, Object clauseWhere, Object returning)
            throws SqlObjectException {
        SqlQueryDelete request = new SqlQueryDelete();
        request.table = identTable;
        if (clauseWhere != null)
            request.insertItem((SqlObject)clauseWhere);
        if (returning != null)
            request.setReturning((TupleExpressions) returning);
        return request;
    }

    private static QualifiedField buildQField(String fieldName)
            throws SqlObjectException {
        if (fieldName.startsWith("`"))
            return new QualifiedField( "", fieldName.substring(1));
        else
            return new QualifiedRField(null, "", fieldName);
    }

    public static SqlQueryInsert buildInsert(Object insert, Object returning)
            throws SqlObjectException {
        SqlQueryInsert request = (SqlQueryInsert) insert;
        request.setReturning((TupleExpressions) returning);
        return request;
    }

    public static SqlQueryInsert buildInsert(String identTable, List<String> columnsList, Object insertValues)
            throws SqlObjectException, SqlParserException{
        SqlQueryInsert request = new SqlQueryInsert();
        request.table = identTable;
        if (insertValues instanceof TupleExpressions) {
            TupleExpressions tuple = (TupleExpressions) insertValues;
            if (tuple.itemsCount() != columnsList.size())
                throw new SqlParserException("В запросе на добавление записи не соответствует количество полей и устанавливаемых значений");
            Preconditions.checkState(tuple.itemsCount() == columnsList.size());
            DMLFieldsAssignments assignments = request.newAssignments();
            DMLFieldAssignment assignment = null;
            for (int i = 0; i < columnsList.size(); i++) {
                assignment = new DMLFieldAssignment(assignments);
                assignment.setField(buildQField(columnsList.get(i)));
                assignment.setExpr((ColumnExpression) tuple.firstSubItem());
            }

        }
        else if (insertValues instanceof Select ){
            // TODO вообще говоря синтаксис позволяет форму "INSERT INTO table1 (SELECT * FROM table2)"
            // но сейчас это не поддержано, требуется перечисление колонок
            Select select = (Select) insertValues;
            if (select.getColumns().itemsCount() != columnsList.size())
                throw new SqlParserException("В запросе на добавление записи не соответствует количество полей и колонок подзапроса");
            DMLFieldsAssignments assignments = request.newAssignments();
            DMLFieldAssignment assignment = null;
            for (int i = 0; i < columnsList.size(); i++) {
                assignment = new DMLFieldAssignment(assignments);
                assignment.setField(buildQField(columnsList.get(i)));
            }
            request.setSelect(select);
        }
        else
            //throw new SqlParserException("Не поддерживаемый раздел значений в запросе request");
        ;
        return request;
    }

    public static Long buildFetchStatement(String s)
            throws SqlParserException{
        try {
            Long value = Long.valueOf(s);
            if (value < 0)
                throw new SqlParserException(
                        String.format(
                                "Ограничение количества строк выборки должно задаваться целым неотрицательным числом (%s)",
                                s
                        )
                );

            return value;
        }
        catch (NumberFormatException nfe_long) {
            throw new SqlParserException(
                    String.format(
                            "Ограничение количества строк выборки должно задаваться целым неотрицательным числом (%s:%s)",
                            s, nfe_long
                    )
            );
        }
    }

    public static class ParsedCursorFetchStatement {
        public long offset;
        public long first;
        public ParsedCursorFetchStatement setOffset(Long value) {
            this.offset = value == null ? 0 : value;
            return this;
        }
        public ParsedCursorFetchStatement setFirst(Long value) {
            this.first = value == null ? 0 : value;
            return this;
        }
    }

    public static CursorSpecification buildCursorSpecification(Object selectStatement, Object clauseOrderBy, ParsedCursorFetchStatement fetchStatement)
            throws SqlObjectException{
        CursorSpecification cursor = new CursorSpecification();
        cursor.setSelect((Select) selectStatement);
        cursor.setOrderBy((OrderBy) clauseOrderBy);
        if (fetchStatement != null) {
            cursor.setFetchOffset(fetchStatement.offset);
            cursor.setFetchFirst(fetchStatement.first);
        }
        return cursor;
    }

    public static OrderBy buildOrderBy(Object orderItem)
            throws SqlObjectException {
        OrderBy orderBy = new OrderBy();
        orderBy.insertItem((SqlObject)orderItem);
        return orderBy;
    }

    public static OrderBy buildOrderBy(Object orderItems, Object orderItem)
            throws SqlObjectException {
        OrderBy orderBy = (OrderBy) orderItems;
        orderBy.insertItem((SqlObject)orderItem);
        return orderBy;
    }

    public static Select buildSelectStatement(Object withClause, Object selectBody)
            throws SqlObjectException {
        Select select = (Select) selectBody;
        select.setWith((CTEsContainer) withClause);
        return select;
    }

    public static Select buildSimpleSelectStatement(Object selectHead, Object columns, Object clauseFrom,
                                                    Object clauseWhere, Object clauseGroup)
            throws SqlObjectException {
        Select select = (Select) selectHead;
        select.setColumns((SelectedColumnsContainer) Preconditions.checkNotNull(columns));
        select.setFrom((FromContainer)clauseFrom);
        select.setWhere((Conditions)clauseWhere);
        select.setGroupBy((GroupBy)clauseGroup);
        return select;
    }

    public static Select buildSelectHead(String comment, boolean distinct) {
        Select select = new Select();
        if (!StringUtils.isEmpty(comment))
            select.hint = comment;
        select.distinct = distinct;
        return select;
    }

    public static Select buidlUnitedSelectStatement(Object selectFirst, String op, Object selectSecond)
            throws SqlObjectException {
        Select first = (Select) selectFirst;
        UnionItem unionItem = new UnionItem(first.newUnions());
        unionItem.setSelect((Select) selectSecond);
        unionItem.unionType = UnionItem.UnionType.fromString(op);
        return first;
    }

    public static SelectedColumnsContainer buildColumns(Object column)
            throws SqlObjectException {
        SelectedColumnsContainer columns = new SelectedColumnsContainer();
        columns.insertItem((SelectedColumn) column);
        return columns;
    }

    public static SelectedColumnsContainer buildColumns(Object listColumns, Object column)
            throws SqlObjectException {
        SelectedColumnsContainer columns = (SelectedColumnsContainer) listColumns;
        columns.insertItem((SelectedColumn) column);
        return columns;
    }

    public static SelectedColumn buildColumn(Object expr, String alias)
            throws SqlObjectException {
        SelectedColumn column = new SelectedColumn();
        column.setExpression((ColumnExpression) expr);
        column.alias = alias;
        return column;
    }

    public static FromContainer buildFromItems(Object fromItem)
            throws SqlObjectException {
        FromContainer from = new FromContainer();
        from.insertItem((FromClauseItem)fromItem);
        return from;
    }

    public static FromContainer buildFromItems(Object firstFromItem, Object tail)
            throws SqlObjectException {
        FromContainer from = (FromContainer) tail;
        from.insertItem((SqlObject)firstFromItem, 0);
        return from;
    }

    public static FromContainer buildTail(Object fromItem)
            throws SqlObjectException {
        FromContainer from = new FromContainer();
        from.insertItem((SqlObject)fromItem);
        return from;
    }

    public static FromContainer buildTail(Object fromItems, Object fromItem)
            throws SqlObjectException {
        FromContainer from = (FromContainer) fromItems;
        from.insertItem((SqlObject)fromItem);
        return from;
    }

    public static FromClauseItem buildJoinedTableExpression(String joinOp, Object fromItem, Object predicate)
            throws SqlObjectException {
        new Join((SqlObject)fromItem, (Predicate) predicate, Join.JoinType.fromString(joinOp));
        return (FromClauseItem) fromItem;
    }

    public static FromClauseItem buildJoinedTableExpression(Object fromItem)
            throws SqlObjectException {
        PredicateIsNull alwaysTrue = new PredicateIsNull();
        alwaysTrue.setExpr(new Expression(Expression.NULL, true));
        alwaysTrue.isRaw = true;
        // эмуляция CROSS_JOIN через INNER JOIN.
        // такая эмуляция может вызывать ошибку в алгоритме JoiningPathBuilder
        // если разбираемый запрос будет объединяться с другими запросами
        // TODO переделать на cross join после его поддержки
        new Join((SqlObject)fromItem, alwaysTrue, Join.JoinType.INNER);
        return (FromClauseItem)fromItem;
    }

    public static FromClauseItem buildTableExpression(String table, String alias)
            throws SqlObjectException {
        Preconditions.checkState(!StringUtils.isEmpty(table));
        FromClauseItem fromItem = new FromClauseItem();
        fromItem.setAlias(alias);
        new SourceTable(fromItem.getTableExpr(), table);
        return fromItem;
    }

    public static FromClauseItem buildTableExpression(Object select, String alias)
            throws SqlObjectException {
        Preconditions.checkState(!StringUtils.isEmpty(alias));
        FromClauseItem fromItem = new FromClauseItem();
        fromItem.setAlias(alias);
        new SourceQuery(fromItem.getTableExpr(), (Select) select);
        return fromItem;
    }

    public static Conditions buildClauseWhere(Object predicate)
            throws SqlObjectException {
        if (predicate instanceof Conditions)
            return (Conditions)predicate;
        else {
            Conditions bracket = new Conditions();
            bracket.insertItem((SqlObject)predicate);
            return bracket;
        }
    }

    public static GroupBy buildClauseGroupBy(Object tuple)
            throws SqlObjectException {
        GroupBy group = new GroupBy();
        group.setTupleItems((TupleExpressions) Preconditions.checkNotNull(tuple));
        return group;
    }

    public static GroupBy buildClauseGroupBy(Object tuple, Object predicate)
            throws SqlObjectException {
        GroupBy group = buildClauseGroupBy(tuple);
        if (predicate instanceof Conditions)
            group.setHaving((Conditions) predicate);
        else {
            Conditions bracket = new Conditions();
            bracket.insertItem((SqlObject)predicate);
            group.setHaving(bracket);
        }
        return group;
    }

    public static CTEsContainer buildWithList(Object withListElement)
            throws SqlObjectException {
        CTEsContainer with = new CTEsContainer();
        with.insertItem((CommonTableExpression) withListElement);
        return with;
    }

    public static CTEsContainer buildWithList(Object withList, Object withListElement)
            throws SqlObjectException {
        CTEsContainer with = (CTEsContainer) withList;
        with.insertItem((CommonTableExpression) withListElement);
        return with;
    }

    public static CommonTableExpression buildCteHead(String ident, List<String> withColumnList, Object selectBody)
            throws SqlObjectException {
        CommonTableExpression cte = new CommonTableExpression();
        cte.alias = ident;
        cte.columns = Preconditions.checkNotNull(withColumnList);
        cte.setSelect((Select) Preconditions.checkNotNull(selectBody));
        return cte;
    }

    public static CommonTableExpression buildCte(Object withListElementHead, Object cycleClause)
            throws SqlObjectException {
        CommonTableExpression cte = (CommonTableExpression) withListElementHead;
        cte.setCycle((CTECycleClause) cycleClause);
        return cte;
    }

    public static CTECycleClause buildCteCycleClause(List<String> cycleColumns,
                                                     String cycleMarkerName,
                                                     String markerCycled,
                                                     String markerDefault) {
        CTECycleClause cycle = new CTECycleClause();
        cycle.setColumns(cycleColumns.toArray(new String[0]));
        cycle.markerCycleColumn = cycleMarkerName;
        cycle.markerCycleValue = markerCycled;
        cycle.markerCycleValueDefault = markerDefault;
        return cycle;
    }

    public static List<String> buildStringList(String ident) {
        List<String> list = new ArrayList<>();
        list.add(ident);
        return list;
    }

    public static List<String> buildStringList(List<String> list, String ident) {
        list.add(ident);
        return list;
    }

    public static List<String> buildFieldsList(String ident, boolean backQuoted) {
        List<String> list = new ArrayList<>();
        if (backQuoted)
            list.add("`" + ident);
        else
            list.add(ident);
        return list;
    }

    public static List<String> buildFieldsList(List<String> list, String ident, boolean backQuoted) {
        if (backQuoted)
            list.add("`" + ident);
        else
            list.add(ident);
        return list;
    }

    public static TupleExpressions buildTuple(Object expr)
            throws SqlObjectException {
        TupleExpressions tuple = new TupleExpressions();
        tuple.insertItem((SqlObject) expr);
        return tuple;
    }

    public static TupleExpressions buildTuple(Object tuple, Object expr)
            throws SqlObjectException {
        TupleExpressions tupleExpr = (TupleExpressions) tuple;
        tupleExpr.insertItem((SqlObject) expr);
        return tupleExpr;
    }

    public static Conditions buildPredicateBracket(Object predicate1, Object predicate2, Conditions.BooleanOp op, boolean isNot)
            throws SqlObjectException {
        if (isNot)
            ((Predicate)predicate2).not = !((Predicate)predicate2).not;
        if (predicate1 instanceof Conditions) {
            Conditions conditions = (Conditions) predicate1;
            if (conditions.booleanOp == op) {
                conditions.insertItem((Predicate) predicate2);
                return conditions;
            }
            else {
                Conditions bracket = new Conditions();
                bracket.booleanOp = op;
                bracket.insertItem((Predicate) predicate1);
                bracket.insertItem((Predicate) predicate2);
                return bracket;
            }
        }
        else {
            Conditions conditions = new Conditions();
            conditions.booleanOp = op;
            conditions.insertItem((Predicate) predicate1);
            conditions.insertItem((Predicate) predicate2);
            return conditions;
        }
    }

    public static PredicateLike buildLike(Object first, Object second, String escape, boolean not)
            throws SqlObjectException {
        PredicateLike predicate = new PredicateLike();
        predicate.setLeft((ColumnExpression) first);
        predicate.setRight((ColumnExpression) second);
        predicate.escape = escape;
        predicate.not = not;
        return predicate;
    }

    public static PredicateForCodeComparison buildCodeCmp(Object first, Object second, PredicateForCodeComparison.ComparisonCodeOperation op)
            throws SqlObjectException {
        PredicateForCodeComparison predicate = new PredicateForCodeComparison();
        predicate.setLeft((ColumnExpression) first);
        predicate.setRight((ColumnExpression) second);
        predicate.comparison = op;
        return predicate;
    }

    public static PredicateBetween buildBetween(Object operand, Object first, Object second, boolean not)
            throws SqlObjectException {
        PredicateBetween predicate = new PredicateBetween();
        predicate.setLeft((ColumnExpression) first);
        predicate.setRight((ColumnExpression) second);
        predicate.setExpr((ColumnExpression) operand);
        predicate.not = not;
        return predicate;
    }

    public static PredicateExists buildExists(Object select)
            throws SqlObjectException {
        PredicateExists predicate = new PredicateExists();
        predicate.setSelect((Select) select);
        return predicate;
    }

    public static PredicateIsNull buildIsNull(Object expr, boolean not)
            throws SqlObjectException {
        PredicateIsNull predicate = new PredicateIsNull();
        predicate.setExpr((ColumnExpression) expr);
        predicate.not = not;
        return predicate;
    }

    public static PredicateInQuery buildInSelect(Object tuple, Object select, boolean not)
            throws SqlObjectException {
        PredicateInQuery predicate = new PredicateInQuery();
        predicate.setSelect((Select) select);
        if (tuple instanceof ColumnExpression)
            predicate.tupleAdd((ColumnExpression) tuple);
        else
            predicate.setTuple((TupleExpressions)tuple);
        predicate.not = not;
        return predicate;
    }

    public static PredicateInTuple buildInTuple(Object expr, Object tuple, boolean not)
            throws SqlObjectException {
        PredicateInTuple predicate = new PredicateInTuple();
        predicate.setExpr((ColumnExpression) expr);
        predicate.setTuple((TupleExpressions) tuple);
        predicate.not = not;
        return predicate;
    }

    public static PredicateComparison buildComparison(Object first, String op, Object second)
            throws SqlObjectException {
        PredicateComparison predicate = new PredicateComparison();
        predicate.setLeft((ColumnExpression) first);
        predicate.setRight((ColumnExpression) second);
        predicate.comparison = PredicateComparison.ComparisonOperation.fromString(op);
        return predicate;
    }

    public static PredicateRegExpMatch buildRegExpMatch(Object source, Object template, String params)
            throws SqlObjectException {
        PredicateRegExpMatch predicate = new PredicateRegExpMatch();
        predicate.setLeft((ColumnExpression) source);
        predicate.setRight((ColumnExpression) template);
        if (!StringUtils.isEmpty(params)) {
            String upperParams = params.toUpperCase();
            predicate.caseSensitive = !upperParams.contains("I"); // 'i'
            predicate.pointAsCRLF= upperParams.contains("S"); //'s'
            predicate.multiLine = upperParams.contains("M"); // 'm'
        }
        return predicate;
    }

    public static Expression buildExprBinary(Object expr1, String op, Object expr2)
            throws SqlObjectException {
        if (expr1 instanceof Expression) {
            Expression expr = (Expression) expr1;
            expr.setExpr(expr.getExpr() + op + Expression.UNNAMED_ARGUMENT_REF);
            expr.insertItem((SqlObject)expr2);
            return expr;
        }
        else {
            Expression expr = new Expression();
            expr.setExpr(Expression.UNNAMED_ARGUMENT_REF + op + Expression.UNNAMED_ARGUMENT_REF);
            expr.insertItem((SqlObject)expr1);
            expr.insertItem((SqlObject)expr2);
            return expr;
        }
    }

    public static ColumnExpression buildExprUnary(String sign, Object expr)
            throws SqlObjectException, SqlParserException{
        String strSign = sign;
        if (strSign.equals("+"))
            // унарный плюс не меняет арифметический результат
            return (ColumnExpression)expr;
        else if (strSign.equals("-")) {
            if (expr instanceof Expression) {
                Expression bracket = (Expression) expr;
                bracket.setExpr("-(" + bracket.getExpr() + ")");
                return bracket;
            }
            //todo для числовых значений было бы достаточно менять знак
            //else if (expr instanceof ValueConst) {}
            else {
                Expression bracket = new Expression();
                bracket.setExpr("-(" + Expression.UNNAMED_ARGUMENT_REF + ")");
                bracket.insertItem((SqlObject)expr);
                return bracket;
            }
        }
        else
            throw new SqlParserException(String.format(SqlParserException.INVALID_UNARY_OPERATION, strSign));
    }

    public static ColumnExpression buildExprBracket(Object expr)
            throws SqlObjectException {
        if (expr instanceof Expression && !((Expression)expr).getExpr().equals(Expression.UNNAMED_ARGUMENT_REF)) {
            // какое-то арифметическое выражение, которое нужно обернуть в скобки
            Expression bracket = new Expression();
            bracket.setExpr("(" + Expression.UNNAMED_ARGUMENT_REF + ")");
            bracket.insertItem((SqlObject)expr);
            return bracket;
        }
        else
            return (ColumnExpression) expr;
    }

    public static Expression buildExprExtractYear(Object exprFrom)
            throws SqlObjectException {
        // В наших выражениях сейчас явно поддержана только функция "EXTRACT(YEAR FROM ...)" - для неё создаём наше выражение.
        // для остальных конструкций (Month, Day, ...) создаём обычное, "чистое" sql-выражение.
        // не есть хорошо, поэтому TODO нужно будет поддержать и выражения "EXTRACT(MONTH/DAY... FROM ...)"
        Expression exprExtract = new Expression(Expr.exprYearFromDate(Expression.UNNAMED_ARGUMENT_REF), false);
        exprExtract.insertItem((SqlObject) exprFrom);
        return exprExtract;
    }

    public static Expression buildExprExtract(String extractPart, Object exprFrom)
            throws SqlObjectException {
        Expression expr = new Expression("EXTRACT(" + extractPart + " FROM " + Expression.UNNAMED_ARGUMENT_REF + ")", true);
        expr.insertItem((SqlObject) exprFrom);
        return expr;
    }

    public static ColumnExpression buildIdentifier(String ident){
        if (ident.startsWith(":")) {
            ParamRef param = new ParamRef();
            param.parameterName = ident.substring(1);
            return param;
        }
        else
            return new QualifiedRField("", ident);
    }

    public static ColumnExpression buildStar(){
        return new QualifiedRField("", "*");
    }

    public static ColumnExpression buildQualifiedIdentifier(String alias, String name, boolean backQuoted)
            throws SqlParserException{
        if (alias.startsWith(":"))
            throw new SqlParserException(String.format(SqlParserException.INVALID_SEMICOLON, alias, name));
        else if (backQuoted)
            return new QualifiedField(alias, name);
        else
            return new QualifiedRField(alias, name);
    }

    public static ColumnExpression buildQualifiedStar(String alias)
            throws SqlParserException {
        return buildQualifiedIdentifier(alias, "*", false);
    }

    public static CaseSimple buildCaseSimpleConditions(Object exprWhen, Object exprThen)
            throws SqlObjectException {
        CaseSimple exprCase = new CaseSimple();
        exprCase.addWhenThen((ColumnExpression) exprWhen, (ColumnExpression) exprThen);
        return exprCase;
    }

    public static CaseSimple buildCaseSimpleConditions(Object caseConditions, Object exprWhen, Object exprThen)
            throws SqlObjectException {
        CaseSimple exprCase = (CaseSimple) caseConditions;
        exprCase.addWhenThen((ColumnExpression) exprWhen, (ColumnExpression) exprThen);
        return exprCase;
    }

    public static CaseSimple buildCaseSimple(Object caseExpr, Object caseConditions, Object caseElse)
            throws SqlObjectException {
        CaseSimple exprCase = (CaseSimple) caseConditions;
        exprCase.setCase((ColumnExpression) caseExpr);
        if (caseElse != null)
            exprCase.setElse((ColumnExpression) caseElse);
        return exprCase;
    }

    public static CaseSearch buildCaseSearchConditions(Object exprWhen, Object exprThen)
            throws SqlObjectException {
        CaseSearch caseExpr = new CaseSearch();
        caseExpr.addWhenThen((Predicate) exprWhen, (ColumnExpression) exprThen);
        return caseExpr;
    }

    public static CaseSearch buildCaseSearchConditions(Object caseConditions, Object exprWhen, Object exprThen)
            throws SqlObjectException {
        CaseSearch exprCase = (CaseSearch) caseConditions;
        exprCase.addWhenThen((Predicate) exprWhen, (ColumnExpression) exprThen);
        return exprCase;
    }

    public static CaseSearch buildCaseSearch(Object caseConditions, Object caseElse)
            throws SqlObjectException {
        CaseSearch exprCase = (CaseSearch) caseConditions;
        if (caseElse != null)
            exprCase.setElse((ColumnExpression) caseElse);
        return exprCase;
    }

    public static Scalar buildExprScalar(Object select)
            throws SqlObjectException {
        Scalar scalar = new Scalar();
        scalar.setSelect(Preconditions.checkNotNull((Select)select));
        return scalar;
    }

    public static Expression buildExprFunction(String ident, Object tuple, boolean distinct)
            throws SqlObjectException {
        Expression expr = new Expression();
        expr.isPureSql = false;
        StringBuilder sb = new StringBuilder();
        sb.append(ident);
        sb.append('(');
        if (distinct)
          sb.append("distinct ");
        boolean isFirst = true;
        if (tuple != null) {
            TupleExpressions args = (TupleExpressions) tuple;
            while (args.isHasChilds()) {
                expr.insertItem(args.firstSubItem());
                if (!isFirst)
                    sb.append(',');
                sb.append(Expression.UNNAMED_ARGUMENT_REF);
                isFirst = false;
            }
        }
        sb.append(')');
        expr.setExpr(sb.toString());
        return expr;
    }

    public static Expression buildExprFunctionOver(Object func, Object part, Object order)
            throws SqlObjectException {
        boolean isFirst;
        // выражение с функции "f([??{,??}])" может уже иметь подчинённые аргументы.
        // дополним строку выражения: "f(...) OVER(PARTITION BY ??{, ??} [ORDER BY ?? [DESC]{, ?? [DESC]}])"
        // и добавим подчинённые
        Expression exprFunc = (Expression) func;

        StringBuilder sb = new StringBuilder(exprFunc.getExpr());
        sb.append(" OVER(");
        if (part != null) {
            TupleExpressions partitionBy = (TupleExpressions) part;
            sb.append("PARTITION BY ");
            isFirst = true;
            while (partitionBy.itemsCount() != 0) {
                if (isFirst)
                    sb.append(", ");
                sb.append(Expression.UNNAMED_ARGUMENT_REF);
                exprFunc.insertItem(partitionBy.firstSubItem());
                isFirst = false;
            }
        }

        if (order != null) {
            OrderBy orderBy = (OrderBy) order;
            OrderByItem orderItem;
            sb.append(" ORDER BY");
            ColumnExpression orderExpr;
            isFirst = true;
            for (SqlObject item : orderBy) {
                orderItem = (OrderByItem) item;
                if (isFirst)
                    sb.append(", ");
                sb.append(Expression.UNNAMED_ARGUMENT_REF);
                orderExpr = orderItem.getExpr();

                exprFunc.insertItem(orderExpr);
                if (orderItem.direction == OrderByItem.OrderDirection.DESC)
                    sb.append(" DESC");

                switch (orderItem.nullOrdering) {
                    case NULLS_FIRST:
                        sb.append(" NULLS FIRST");
                        break;
                    case NULLS_LAST:
                        sb.append(" NULLS LAST");
                        break;
                }
                isFirst = false;
            }
        }
        sb.append(")"); // OVER(
        exprFunc.setExpr(sb.toString());
        return exprFunc;
    }

    public static ValueConst buildValueCode(String s) { return new ValueConst(CodeValue.valueOf(s)); }

    public static ValueConst buildValueDate(String s)
            throws ValuesSupport.ValueException {
        return ValueConst.ofDate(ValuesSupport.parseDate(s, "yyyy-MM-dd"));
    }

    public static ValueConst buildValueTime(String s)
            throws ValuesSupport.ValueException{
        ValueConst value = ValueConst.ofTime(ValuesSupport.parseTime(s, "HH:mm:ss"));
        return value;
    }

    public static ValueConst buildValueTimeStamp(String s)
            throws ValuesSupport.ValueException{
        ValueConst value = ValueConst.ofDateTime(ValuesSupport.parseDateTime(s, "yyyy-MM-dd HH:mm:ss"));
        return value;
    }

    public static ValueConst buildValueNumber(String s) {
        try {
            Long value = Long.valueOf(s);
            if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE)
                return new ValueConst(value.intValue());
            else
                return new ValueConst(value);
        }
        catch (NumberFormatException nfe_long) {
            return new ValueConst(Double.valueOf(s));
        }
    }

    public static String buildComment(String c) {
        if (c.startsWith("--"))
            return c.substring(3);
        else
            return c.substring(3, c.length() - 2);
    }
}
