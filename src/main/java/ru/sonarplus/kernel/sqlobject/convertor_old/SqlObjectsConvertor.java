package ru.sonarplus.kernel.sqlobject.convertor_old;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.DoubleQuoteUtils;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsDbSupportUtils;
import ru.sonarplus.kernel.sqlobject.expressions.ExprUtils;
import ru.sonarplus.kernel.sqlobject.expressions.ExpressionException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public abstract class SqlObjectsConvertor {

	boolean isNeedUnnamedParameters = true;

	private SqlObjectsDbSupportUtils dbSupport = null;
	
	protected abstract SqlObjectsDbSupportUtils createDBSupport();

    public SqlObjectsDbSupportUtils getDBSupport() {
        if (dbSupport == null)
            dbSupport = createDBSupport();
        return dbSupport;
    }

	protected  String getHint(SqlQuery request) {
		String result = null;
        if (request instanceof DataChangeSqlQuery)
			result = ((DataChangeSqlQuery)request).hint;
		else if (request instanceof CursorSpecification || request instanceof Select)
			result = SqlObjectUtils.getRequestHint(request);
		if (!StringUtils.isEmpty(result))
			return "/* " + result + " */";
		else
		  return "";
	}
	
/*	protected static String StdNameToNativeName(String name) {
		return name;
	}
*/	
	
	protected abstract String convertCallSp(CallStoredProcedure sqlObject)
            throws SqlObjectException;
    
	
	public SqlObjectsConvertor() {

	}

	public String convertValue(Value value) throws SqlObjectException {
        return getDBSupport().expressionStrForValue(value);
	}
	protected SqlSelectExpression emptySqlExpression() {
		return new SqlSelectExpression(); 
	}
	
	protected String returningAsString(SqlObject returning) throws SqlObjectException {
		if (returning == null) {
			return "";
		}
		String result = "";
		for (SqlObject item: returning) {
			if (!StringUtils.isEmpty(result)) {
				result += ", ";
			}
			result += convertColumnExpression((ColumnExpression) item);
		}
		return result;
	}
	
	protected String convertConditionsContainer(Conditions conditions, SqlSelectExpression expression)
			throws SqlObjectException {
		
		if (conditions.isEmpty()) {
			return "";
		}
		String booleanOp = "";
		switch (conditions.booleanOp) {
			case AND:
				booleanOp = " AND ";
				break;
			case OR:
				booleanOp = " OR ";
				break;
			default:	
				Preconditions.checkArgument(false, "Неизвестная логическая операция");
		}
		boolean isNeedBracket = conditions.itemsCount() > 1;
		StringBuilder result = new StringBuilder();
		for (SqlObject item: conditions) {
			if (result.length() > 0) {
				result.append(booleanOp);
			}
			result.append(convertConditions((Predicate) item, expression, 
						isNeedBracket ));
		}
		if (conditions.not) {
			result.insert(0, "NOT(");
			result.append(')');
		}
		return result.toString();
	}
	
	public String unionTypeToString(UnionItem.UnionType unionType) {
		switch (unionType) {
			case UNION:
				return "union";
			case UNION_ALL:
				return "union all";
			case INTERSECT:
				return "intersect";
			case MINUS:
				return "minus";
			default:
				Preconditions.checkArgument(false);
				return null;
		}
		
	}
	
	protected String convertParameter(Parameter parameter) {
		if (isNeedUnnamedParameters) {
			return "?";
		}
		else {
			return Expression.CHAR_BEFORE_PARAMETER + parameter.parameterName;
		}
	}
	
	protected String convertArg(ColumnExpression arg) throws SqlObjectException {
	    Preconditions.checkNotNull(arg);
		if (arg instanceof QualifiedField) {
			return convertQualifiedField((QualifiedField) arg);
		}
		else if (arg instanceof Value) {
			return convertValue((Value) arg);
		}
		else if (arg instanceof Expression) {
			return convertExpression((Expression) arg);
		}
		else if (arg instanceof Parameter) {
			return convertParameter((Parameter) arg);
		}
		else if (arg instanceof Scalar) {
			return convertScalar((Scalar) arg);
		}
        else if (arg instanceof CaseSearch)
            return convertCaseSearch((CaseSearch)arg);
        else if (arg instanceof CaseSimple)
            return convertCaseSimple((CaseSimple)arg);
		else {
            Preconditions.checkArgument(false, arg.getClass().getName());
			return null;
		}
	}
	
	protected String convertExpression(Expression expression) throws SqlObjectException {
        String strExpr = expression.getExpr();
        Preconditions.checkNotNull(strExpr);
        if (ExprUtils.getUnnamedRefCount(expression.getExpr()) != expression.itemsCount())
              throw new ExpressionException(String.format(ExprUtils.NOT_CORRESPONDING_UNNAMED_ARG_REFS_WITH_EXPR_CHILDS, strExpr));
        if (!expression.isHasChilds())
            return strExpr;
        int unnamedRefIndex = 0;
        int posCurrent = 0;
        int posBuildFrom = posCurrent;
        boolean isLastPartBuilded = false;
        StringBuilder sb = new StringBuilder();
        while (posCurrent < strExpr.length()) {
            switch (strExpr.charAt(posCurrent)) {
                case '\'':
                    posCurrent = ExprUtils.getPosAfterLiteral(strExpr, posCurrent);
                    isLastPartBuilded = false;
                    break;
                case Expression.CHAR_BEFORE_TOKEN:
                    Preconditions.checkArgument(
                            strExpr.substring(posCurrent, posCurrent + Expression.UNNAMED_ARGUMENT_REF.length())
                                    .equals(Expression.UNNAMED_ARGUMENT_REF));
                    sb.append(strExpr, posBuildFrom, posCurrent);
                    posCurrent += Expression.UNNAMED_ARGUMENT_REF.length();
                    posBuildFrom = posCurrent;
                    sb.append(convertArg((ColumnExpression)expression.getItem(unnamedRefIndex)));
                    unnamedRefIndex++;
                    isLastPartBuilded = true;
                    break;
                default:
                    posCurrent++;
                    isLastPartBuilded = false;
            }
        }
        if (!isLastPartBuilded)
            sb.append(strExpr, posBuildFrom, posCurrent);
        return sb.toString();
	}
	
	protected String convertCaseSimple(CaseSimple value) throws SqlObjectException {
        // TODO при конвертации в sql конструкция case оборачивается в скобки: "(CASE...END)". Наверное это необязательно.
        // но если скобки не ставить - нужно будет править тесты
        StringBuilder result = new StringBuilder("(CASE "+ convertColumnExpression(value.getCase()) + " ");
        for (SqlObject item: value)
            if (item instanceof CaseSimple.WhenThen) {
                CaseSimple.WhenThen simpleCase = (CaseSimple.WhenThen) item;
                result.append("WHEN " +
                    convertColumnExpression(simpleCase.getWhen()) +
                    " THEN "+convertColumnExpression(simpleCase.getThen()) + " ");
            }
		ColumnExpression elseExpr = value.getElse();
		if (elseExpr != null) {
			result.append("ELSE " + convertColumnExpression(elseExpr) + " ");
		}
		result.append("END)");
		return result.toString();
	}
	
	protected String convertCaseSearch(CaseSearch value) throws SqlObjectException {
		StringBuilder result = new StringBuilder("(CASE ");
		for (SqlObject item: value) {
			if (item instanceof CaseSearch.WhenThen) {
				CaseSearch.WhenThen searchCase =
						(CaseSearch.WhenThen) item;
				result.append("WHEN " + 
						convertConditions(searchCase.getWhen()) +
						" THEN "+convertColumnExpression(searchCase.getThen()) + " ");
			}
		}
		ColumnExpression elseExpr = value.getElse();
		if (elseExpr != null) {
			result.append("ELSE " + convertColumnExpression(elseExpr) + " ");
		}
		result.append("END)");
		return result.toString();
	}
	
	protected String convertCase(Case value) throws SqlObjectException {
		if (value instanceof CaseSimple) {
			return convertCaseSimple((CaseSimple) value);
		}
		else if (value instanceof CaseSearch) {
			return convertCaseSearch((CaseSearch) value);
		}
		else {
			Preconditions.checkArgument(false, 
					"Неподдерживаемая конструкция CASE - "+
							value.getClass().getSimpleName());
			return null;
		}
	}
	
	protected String convertScalar(Scalar scalar)
            throws SqlObjectException {
		return "("+convertQuerySelect(scalar.findSelect()) + ")";
	}
	
	protected String convertColumnExpression(ColumnExpression expression)
			throws SqlObjectException {
		if (expression == null) {
			return "";
		}
		if (expression instanceof QualifiedField) {
			return convertQualifiedField((QualifiedField) expression);
		}
		else if (expression instanceof Parameter) {
			return convertParameter((Parameter) expression);
		} 
		else if (expression instanceof Value) {
			return convertValue((Value) expression);
		} 
		else if (expression instanceof Expression) {
			return convertExpression((Expression) expression);
		} 
		else if (expression instanceof Case) {
			return convertCase((Case) expression);
		} 
		else if (expression instanceof Scalar) {
			return convertScalar((Scalar) expression);
		}
		else {
			Preconditions.checkArgument(false, "convertColumnExpression неизвестный аргумент %s",
					expression.getClass().getSimpleName());
			return null;
		}
	}
	
	protected String convertExpr(ColumnExpression expr)
            throws SqlObjectException {
		Preconditions.checkArgument(expr != null);
		return convertColumnExpression(expr);
	}
	
	public static String comparisonToString(PredicateComparison.ComparisonOperation operation) {
		switch (operation) {
		case EQUAL:
			return "=";
		case NOT_EQUAL:
			return "<>";
		case LESS:
			return "<";
		case GREAT:
			return ">";
		case LESS_EQUAL:
			return "<=";
		case GREAT_EQUAL:
			return ">=";
		default:
			Preconditions.checkArgument(false,"Неизвестная операция сравнения");
			return null;
		}
	}
	
	protected String convertPredicateComparison(PredicateComparison predicate,
			SqlSelectExpression expression)
            throws SqlObjectException {
		if ((predicate.getLeft() == null) || (predicate.getRight() == null)) {
			return "";
		}
		return convertExpr(predicate.getLeft()) + 
				comparisonToString(predicate.comparison) +
				convertExpr(predicate.getRight());
	}
	
	protected String convertPredicateBetween(PredicateBetween predicate)
            throws SqlObjectException {
		return "(" + convertColumnExpression(predicate.getExpr()) +
				" BETWEEN " + 
				convertColumnExpression(predicate.getLeft()) +
				" AND " +
				convertColumnExpression(predicate.getRight())
				+ ")";
	}
	
	protected String convertPredicateLike(PredicateLike predicate)
            throws SqlObjectException {
		StringBuilder result = new StringBuilder(
				convertColumnExpression(predicate.getExpr()));
		result.append(" LIKE ");
		result.append(convertColumnExpression(predicate.getTemplate()));
		if (!StringUtils.isEmpty(predicate.escape)) {
			result.append(" ESCAPE ");
			result.append("'");
			result.append(predicate.escape.replaceAll("'", "''"));
			result.append("'");
		}
		return result.toString(); 
	}
	
	protected abstract String convertPredicateRegExpMatch(PredicateRegExpMatch predicate) throws SqlObjectException;
	
	protected String convertPredicateIsNull(PredicateIsNull predicate)
            throws SqlObjectException {
		return convertColumnExpression(predicate.getExpr()) + " IS NULL";
	}
	
	protected String convertPredicateInTuple(PredicateInTuple predicate)
            throws SqlObjectException {
		StringBuilder result = new StringBuilder(convertColumnExpression(predicate.getExpr()));
		result.append(" IN (");
		boolean first = true;
		for (SqlObject item : predicate.getTuple()) {
			if (first) {
				first = false;
			}
			else {
				result.append(",");
			}
			result.append(convertColumnExpression((ColumnExpression) item));
		}
		result.append(")");
		return result.toString();
	}
	
	protected String convertSubReq(PredicateInQuery predicate)
            throws SqlObjectException {
		Select select = Preconditions.checkNotNull(predicate.findSelect(), "Подзапрос в условии '..in (select...)' не задан");
		return convertQuerySelect(select);
	}
	
	protected String convertPredicateInQuery(PredicateInQuery predicate)
            throws SqlObjectException {
		StringBuilder result = new StringBuilder("(");
		boolean first = true;
		for (SqlObject item: predicate.getTuple()) {
			if (first) {
				first = false;
			}
			else {
				result.append(',');
			}
			result.append(convertColumnExpression((ColumnExpression) item));
		}
		result.append(") IN (");
		result.append(convertSubReq(predicate));
		result.append(')');
		return result.toString();
	}
	
	protected String convertPredicateIn(PredicateIn predicate)
            throws SqlObjectException {
		if (predicate instanceof PredicateInTuple) {
			return convertPredicateInTuple( (PredicateInTuple) predicate);
		}
		else if (predicate instanceof PredicateInQuery) {
			return convertPredicateInQuery( (PredicateInQuery) predicate);
		}
		else {
			Preconditions.checkArgument(false,
					"Неизвестный тип условия %s (TPredicateIn)",
					predicate.getClass().getSimpleName());
			return null;
		}
	}
	
	protected String convertPredicateExists(PredicateExists predicate)
            throws SqlObjectException {
		Select select = predicate.getSelect();
		Preconditions.checkNotNull(select);
		return "EXISTS("+convertQuerySelect(select) + ")";
	}
	
	protected String convertPredicate(Predicate predicate, 
			SqlSelectExpression expression)
            throws SqlObjectException {
		String result = "";
		if (predicate instanceof PredicateComparison) {
			result = convertPredicateComparison( (PredicateComparison) predicate,
					expression);
		}
		else if (predicate instanceof PredicateBetween) {
			result = convertPredicateBetween((PredicateBetween) predicate);
		}
		else if (predicate instanceof PredicateLike) {
			result = convertPredicateLike((PredicateLike) predicate);
		} 
		else if (predicate instanceof PredicateRegExpMatch) {
			result = convertPredicateRegExpMatch((PredicateRegExpMatch) predicate);
		} 
		else if (predicate instanceof PredicateIsNull) {
			result = convertPredicateIsNull((PredicateIsNull) predicate);
		} 
		else if (predicate instanceof PredicateIn) {
			result = convertPredicateIn((PredicateIn) predicate);
		} 
		else if (predicate instanceof PredicateExists) {
			result = convertPredicateExists((PredicateExists) predicate);
		}
		else {
			Preconditions.checkArgument(false, "convertPredicate подан неизвестный аргумент");
		}
		if (predicate.not) {
			result = "NOT(" + result + ")";
		}
		return result;
	}
	
	
	protected String convertConditions(Predicate predicate, SqlSelectExpression expression,
			boolean isNeedBracket)
            throws SqlObjectException {
		if (predicate == null) {
			return "";
		}
		
		if (predicate instanceof Conditions) {
			String result = convertConditionsContainer((Conditions) predicate, expression);
			if (isNeedBracket && !StringUtils.isEmpty(result)) {
				result = "(" + result + ")";
			}
			return result;
		}
		else {
			return convertPredicate(predicate, expression);
		}
		
	}
	
	protected String convertConditions(Predicate predicate, SqlSelectExpression expression)
            throws SqlObjectException {
		return convertConditions(predicate, expression, false);
	}
	
	protected String convertConditions(Predicate predicate)
            throws SqlObjectException {
		return convertConditions(predicate, new SqlSelectExpression());
	}
	
	protected void convertWhere(Conditions conditions, SqlSelectExpression sqlExpression)
            throws SqlObjectException {
		sqlExpression.where = convertConditions(conditions, sqlExpression);
	}
	
	
	protected String convertQueryDelete(SqlQueryDelete query)
            throws SqlObjectException {
		SqlSelectExpression sqlExpr = new SqlSelectExpression();
		Conditions where = query.findWhere();
		String whereStr = convertConditions(where, sqlExpr);
		return "DELETE " + getHint(query) + "FROM " + query.table +
				(StringUtils.isEmpty(whereStr) ? "" : " WHERE "+ whereStr);
	}
	
	protected String dmlFieldsAsString(DMLFieldsAssignments assignments) {
		StringBuilder result = new StringBuilder();
		for (SqlObject item: assignments) {
			DMLFieldAssignment assignment = (DMLFieldAssignment) item; 
			if (result.length() > 0) {
				result.append(',');
			}
			result.append(convertQualifiedField(assignment.getField()));
		}
		return result.toString();
	}

	
	protected String dmlValuesAsString(DMLFieldsAssignments assignments)
            throws SqlObjectException {
		StringBuilder result = new StringBuilder();
		for (SqlObject item: assignments) {
			DMLFieldAssignment assignment = (DMLFieldAssignment) item; 
			if (result.length() > 0) {
				result.append(',');
			}
			result.append(convertColumnExpression(assignment.getExpr()));
		}
		return result.toString();
		
	}
	
	protected String convertQueryInsert(SqlQueryInsert insertQuery)
            throws SqlObjectException {
		StringBuilder result = new StringBuilder(
				"INSERT " + getHint(insertQuery) + "INTO " + 
		         insertQuery.table);
		String fields = dmlFieldsAsString(insertQuery.getAssignments());
		if (!StringUtils.isEmpty(fields)) {
			result.append('(');
			result.append(fields);
			result.append(')');
		}
		result.append(' ');
		Select select = insertQuery.findSelect();
		if (select == null) {
			result.append("VALUES("+dmlValuesAsString(insertQuery.getAssignments())+")");
		}
		else {
			result.append("("+convertQuerySelect(select)+")");
		}
		return result.toString();
	}
	
	protected String convertQueryUpdate(SqlQueryUpdate updateQuery)
            throws SqlObjectException {
		StringBuilder setValues = new StringBuilder();
		for (SqlObject item: updateQuery.getAssignments()) {
			DMLFieldAssignment assignment = (DMLFieldAssignment) item; 
			if (setValues.length() > 0) {
				setValues.append(", ");
			}
			setValues.append(convertQualifiedField(assignment.getField()));
			setValues.append('=');
			setValues.append(convertColumnExpression(assignment.getExpr()));
		}
		Preconditions.checkArgument(setValues.length() > 0);
		Conditions where = updateQuery.findWhere();
		SqlSelectExpression expression = new SqlSelectExpression();
		String whereStr = convertConditions(where, expression);
		StringBuilder result = new StringBuilder("UPDATE " +
				getHint(updateQuery)+updateQuery.table +
				" SET "+setValues.toString()
				);
		if (!StringUtils.isEmpty(whereStr)) {
			result.append(" WHERE "+whereStr);
		}
		return result.toString();
	}
	
	protected String convertQueryDataModify(DataChangeSqlQuery query)
            throws SqlObjectException {
		String result = "";
		String vars = "";
		
		String returning = returningAsString(query.getReturning());
		if (!StringUtils.isEmpty(returning)) {
			vars = query.getIntoVariables();
		}
		
		if (query instanceof SqlQueryDelete) {
			result = convertQueryDelete( (SqlQueryDelete) query);
		}
		else if (query instanceof SqlQueryInsert) {
			result = convertQueryInsert( (SqlQueryInsert) query);
		}
		else if (query instanceof SqlQueryUpdate) {
			result = convertQueryUpdate( (SqlQueryUpdate) query);
		}
		else {
			Preconditions.checkArgument(false, 
					"ConvertQueryDataModify: неизвестный тип запроса");
		}
		if (!StringUtils.isEmpty(returning)) {
			result += " RETURNING " + returning;
		}
		if (!StringUtils.isEmpty(vars)) {
			result += " INTO " + vars;
		}
		return result;
	}
	
	protected abstract String convertTransactionCommand(SqlTransactionCommand command);
	
	public String internalConvert(SqlObject sqlObject) {
		if (sqlObject == null) {
			return "";
		}	
		else if (sqlObject instanceof CallStoredProcedure) {
			return convertCallSp( (CallStoredProcedure) sqlObject);
		}
		else if (sqlObject instanceof Select) {
			return convertQuerySelect( (Select) sqlObject);
		}
		else if (sqlObject instanceof CursorSpecification) {
			return convertCursorSpecification( (CursorSpecification) sqlObject);
		} 
		else if (sqlObject instanceof DataChangeSqlQuery) {
			return convertQueryDataModify( (DataChangeSqlQuery) sqlObject);
		}
		else if (sqlObject instanceof SqlTransactionCommand) {
			return convertTransactionCommand( (SqlTransactionCommand) sqlObject);
		}
		else {
			return "";
		}
	}
	
	protected String convertQuerySelect(Select select)
            throws SqlObjectException {
		SqlSelectExpression sql = convertSelectClauses(select);
		return sql.asString(select.distinct, getHint(select));
	}
	
	public String convert(SqlObject sqlObject) {
		return internalConvert(sqlObject);
	}
	
	protected String convertColumn(SelectedColumn column)
            throws SqlObjectException {
		return convertColumnExpression(column.getColExpr()) +
				(!StringUtils.isEmpty(column.alias) ? " " +
						DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(column.alias) : "");
	}
	
	protected void convertColumns(SelectedColumnsContainer columns,
			SqlSelectExpression expression) throws SqlObjectException {
		if (columns == null) {
			return;
		}
		StringBuilder sb = new StringBuilder();

		for (SqlObject item: columns) {
			String column = convertColumn((SelectedColumn) item);
			Preconditions.checkState(column.length() > 0,
					"SqlObjectsConvertor.ConvertColumns: не сконвертировалась колонка");
            if (sb.length() != 0)
                sb.append(",");
            sb.append(column);
		}
        expression.columns = sb.toString();
	}
	
	protected boolean isJoin(FromClauseItem fromClauseItem) {
		return fromClauseItem.getJoin() != null;
	}
	
	protected String convertSourceTable(SourceTable source) {
		String result = source.table == null ? "" : source.table.trim();
		Preconditions.checkArgument(!result.equals(""));
		return result;
	}
	
	protected String convertSourceQuery(SourceQuery source)
            throws SqlObjectException {
		Select select = source.findSelect();
		Preconditions.checkArgument(select != null);
		return "(" + convertQuerySelect(select) + ")";
	}
	
	protected String convertSource(Source source)
            throws SqlObjectException {
		Preconditions.checkArgument(source != null);
		if (source instanceof SourceTable) {
			return convertSourceTable( (SourceTable) source);
		}
		else if (source instanceof SourceQuery) {
			return convertSourceQuery( (SourceQuery) source);
		}
		else {
			Preconditions.checkArgument(false, 
					"В convertSource передан экземпляр класса %s",
					source.getClass().getSimpleName());
			return "";
		}	
	}
	
	protected String convertTableExpression(TableExpression expression)
            throws SqlObjectException {
		Preconditions.checkArgument(expression != null);
		StringBuilder result = new StringBuilder(convertSource(expression.getSource()));
		if (!StringUtils.isEmpty(expression.alias)) {
			result.append(' ');
			result.append(DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(expression.alias));
		}
		return result.toString();
	}
	
	public String joinTypeToStringForSql(Join.JoinType value) {
		switch (value) {
		case LEFT:
			return "left";
		case RIGHT:
			return "right";
		case INNER:
			return "inner";
		case FULL_OUTER:
			return "full outer";
		default:
			Preconditions.checkArgument(false);
			return null;
		}
	}
	
	protected String convertFromClauseItem(FromClauseItem fromClauseItem,
			SqlSelectExpression expression) throws SqlObjectException {
		
		String tableExpr = convertTableExpression(fromClauseItem.getTableExpr());
		if (isJoin(fromClauseItem)) {
			return joinTypeToStringForSql(fromClauseItem.getJoin().joinType) +
					" JOIN " + tableExpr + " ON " +
					convertConditions(fromClauseItem.getJoin().getJoinOn(),
							expression);
		}
		else {
			return tableExpr;
		}
	}
	
	protected void ConvertFromContainer(FromContainer fromContainer,
			SqlSelectExpression expression) throws SqlObjectException {
		
		if (fromContainer == null) {
			return;
		}
		
		String from = "";
		String joins = "";
		for (SqlObject item: fromContainer) {
			String fromItem = convertFromClauseItem((FromClauseItem) item, expression);
			if (isJoin((FromClauseItem) item)) {
				if (!joins.equals("")) {
					joins += " ";
				}
				joins += fromItem;
			}
			else {
				if (!from.equals("")) {
					from += ",";
				}
				from += fromItem;
			}
		}
		expression.from = from;
		if (!joins.equals("")) {
			expression.from += " " + joins;
		}
	}
	
	protected void convertFromContainer(FromContainer fromContainer,
			SqlSelectExpression expression) throws SqlObjectException {
		if (fromContainer == null) {
			return;
		}
		StringBuilder from = new StringBuilder();
		StringBuilder joins = new StringBuilder();
		for (SqlObject item: fromContainer) {
			String fromItem = convertFromClauseItem((FromClauseItem)item, expression);
			if (isJoin((FromClauseItem) item )) {
				if (joins.length() > 0) {
					joins.append(" ");
				}
				joins.append(fromItem);
			}
			else {
				if (from.length() > 0) {
					from.append(",");
				}
				from.append(fromItem);
			}
		}
		if (joins.length() > 0) {
			from.append(" ");
			from.append(joins.toString());
		}
		expression.from = from.toString();
	}
	
	protected String checkFillingFromClause(String value) {
		return value;
	}
	
	protected String convertGroupBy(GroupBy groupBy) throws SqlObjectException {
		if (groupBy == null) {
			return "";
		}
		StringBuilder result = new StringBuilder();
		for (SqlObject item: groupBy.getTupleItems()) {
			if (result.length() > 0) {
				result.append(", ");
			}
			result.append(convertColumnExpression((ColumnExpression) item));
		}
		if (result.length() > 0) {
			Conditions having = groupBy.getHaving();
			if (having != null) {
				String havingStr = convertConditions(having, new SqlSelectExpression());
				if (!StringUtils.isEmpty(havingStr)) {
					result.append(" HAVING ");
					result.append(havingStr);
				}
			}
		}
		return result.toString();
	}
	
	protected String convertCTECycle(CTECycleClause cycle) {
		if (cycle == null) {
			return "";
		}
		StringBuilder result = new StringBuilder();
		for (String column: cycle.columns) {
			if (result.length() > 0) {
				result.append(", ");
			}
			result.append(DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(column));
		}
		result.insert(0, "CYCLE ");
		result.append(" SET " + cycle.markerCycleColumn + " TO '" + cycle.markerCycleValue + "' DEFAULT '" +
				cycle.markerCycleValueDefault + "'");
		if (!StringUtils.isEmpty(cycle.columnPath)) {
			result.append(" USING " + cycle.columnPath);
		}
		return result.toString();
	}
	
	protected String convertCTE(CommonTableExpression cte)
            throws SqlObjectException {
		StringBuilder result = new StringBuilder();
		for (String column : cte.columns) {
			if (result.length() > 0) {
				result.append(',');
			}
			result.append(DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(column));
		}
		Preconditions.checkArgument(result.length() > 0);
		result.insert(0, DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(cte.alias) + '(');
		result.append(") AS ("+convertQuerySelect(cte.getSelect()) +")");
		result.append(convertCTECycle(cte.getCycle()));
		return result.toString();
	}
	
	protected String convertWith(CTEsContainer container)
            throws SqlObjectException {
		if (container == null) {
			return "";
		}
		StringBuilder result = new StringBuilder();
		for (SqlObject item: container) {
			if (result.length() > 0) {
				result.append(',');
			}
			result.append(convertCTE((CommonTableExpression) item));
		}
		if (result.length() > 0) {
			result.insert(0, "WITH ");
		}
		return result.toString();
	}

    protected String convertUnionItem(UnionItem union)
            throws SqlObjectException {
        if (union == null) {
            return "";
        }
        Select select = union.findSelect();
        Preconditions.checkNotNull(select);

        StringBuilder result = new StringBuilder();
        result.append(' ');
        result.append(unionTypeToString(union.unionType).toUpperCase());
        result.append(' ');
        UnionsContainer container = union.findSelect().findUnions();
        boolean addBrackets = (container != null) && container.isHasChilds();
        if (addBrackets) {
            result.append('(');
        }
        result.append(convertQuerySelect(select));
        if (addBrackets) {
            result.append(')');
        }
        return result.toString();

    }

	protected String convertUnions(UnionsContainer unions)
            throws SqlObjectException {
		if ((unions == null) || !unions.isHasChilds()) {
			return "";
		}
		StringBuilder result = new StringBuilder();
		for (SqlObject item: unions) {
			result.append(convertUnionItem((UnionItem) item));
		}
		return result.toString();
	}
	
	public SqlSelectExpression convertSelectClauses(Select select)
            throws SqlObjectException {
		SqlSelectExpression result = emptySqlExpression();
		SelectedColumnsContainer columns = select.getColumns();
		convertColumns(columns, result);
		if (StringUtils.isEmpty(result.columns)) {
			result.columns = "*";
		}

		FromContainer from = select.findFrom();
		convertFromContainer(from, result);
		result.from = checkFillingFromClause(result.from);

		convertWhere(select.findWhere(), result);

		result.groupBy = convertGroupBy(select.findGroupBy());

		result.with = convertWith(select.findWith());

		result.unions = convertUnions(select.findUnions());

		return result;
	}
	
	protected String orderDirectionToStr(OrderByItem.OrderDirection direction) {
		switch (direction) {
			case ASC:
				return "asc";
			case DESC:
				return "desc";
			default:
				Preconditions.checkArgument(false);
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
				Preconditions.checkArgument(false);
				return null;
		}
	}

	protected String convertOrderByItem(OrderByItem orderByItem)
            throws SqlObjectException {
		StringBuilder result = new StringBuilder(
				convertColumnExpression(orderByItem.getExpr()));
		result.append(" ");
		result.append(orderDirectionToStr(orderByItem.direction));
		if (orderByItem.nullOrdering != OrderByItem.NullOrdering.NONE) {
			result.append(nullOrderingToStr(orderByItem.nullOrdering));
		}
		return result.toString();
	}
	
	protected String convertOrderBy(OrderBy orderBy)
            throws SqlObjectException {
		if (orderBy == null) {
			return "";
		}
		StringBuilder result = new StringBuilder();
		for (SqlObject item: orderBy) {
			if (result.length() > 0) {
				result.append(", ");
			}
			result.append(convertOrderByItem((OrderByItem) item));
		}
		return result.toString();
	}
	
	protected String getOrderByClause(CursorSpecification cursor)
            throws SqlObjectException {
		OrderBy orderBy = cursor.findOrderBy();
		if (orderBy == null) {
			return "";
		}
		String result = convertOrderBy(orderBy).trim();
		if (!StringUtils.isEmpty(result)) {
			result = " ORDER BY "+result;
		}
		return result;
	}
	
	protected String getFetchLimit(CursorSpecification cursor) {
		if (cursor.getFetchFirst() == 0L) {
			return "";
		}
		else {
			return " LIMIT " + cursor.getFetchFirst();
		}
	}
	
	public String convertCursorSpecification(CursorSpecification cursor) {
		Select select = cursor.getSelect();
		SqlSelectExpression sql = convertSelectClauses(select);
		return sql.asString(select.distinct, SqlObjectUtils.getRequestHint(select)) +
				getOrderByClause(cursor) + getFetchLimit(cursor);
	}
	
	protected String convertQualifiedField(QualifiedField field) {
		Preconditions.checkArgument(!( 
				StringUtils.isEmpty(field.fieldName) ||
		(field instanceof QualifiedRField)));
		StringBuilder result = new StringBuilder(DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(
				field.fieldName));
		if (!StringUtils.isEmpty(field.alias)) {
			result.insert(0, DoubleQuoteUtils.doubleQuoteIdentifierIfNeed(field.alias)+".");
		}
		return result.toString();
	}

	
	
	
}
