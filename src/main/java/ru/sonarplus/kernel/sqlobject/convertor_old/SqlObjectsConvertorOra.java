package ru.sonarplus.kernel.sqlobject.convertor_old;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsDbSupportUtils;
import ru.sonarplus.kernel.sqlobject.db_support_ora.SqlObjectsDbSupportUtilsOra;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public class SqlObjectsConvertorOra extends SqlObjectsConvertor {

	public SqlObjectsConvertorOra() {

	}

    @Override
    protected SqlObjectsDbSupportUtils createDBSupport() {
        return new SqlObjectsDbSupportUtilsOra();
    }

    @Override
	protected String convertCallSp(CallStoredProcedure sqlObject)
			throws SqlObjectException {
		StringBuilder result = new StringBuilder("CALL "+sqlObject.spName);
		boolean first = true;
		for (SqlObject item: sqlObject.getTuple()) {
			if (first) {
				first = false;
				result.append('(');
			}
			else {
				result.append(',');
			}
			result.append(convertColumnExpression((ColumnExpression) item));
		}
		if (!first) {
			result.append(')');
		}
		return result.toString();
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

	@Override
	protected String convertPredicateRegExpMatch(PredicateRegExpMatch predicate)
            throws SqlObjectException {
		return "REGEXP_LIKE(" + convertColumnExpression(predicate.getExpr())+
				","+convertColumnExpression(predicate.getTemplate()) + 
				","+
				"'"+matchParammeter(predicate).replaceAll("'","''")+"')";
	}

	@Override
	protected String convertTransactionCommand(SqlTransactionCommand command) {
		SqlTransactionCommand.checkTransactionCommand(command);
		switch (command.command) {
			case SET_TRANSACTION:
				StringBuilder result = new StringBuilder("SET TRANSACTION");
				if (!StringUtils.isEmpty(command.statement)) {
					result.append(' ');
					result.append(command.statement);
				}
				return result.toString();
			case ROLLBACK:
				return "ROLLBACK";
			case COMMIT: 
				return "COMMIT";
			case SET_SAVEPOINT:
				return "SAVEPOINT "+command.statement;
			case ROLLBACK_TO:
				return "ROLLBACK TO "+command.statement;
			case REMOVE_SAVEPOINT:
				return "RELEASE SAVEPOINT "+command.statement;
			default:
				Preconditions.checkArgument(false);
				return null;
		}
	}
	
	@Override
	protected String checkFillingFromClause(String value) {
		if (StringUtils.isEmpty(value)) {
			return "DUAL";
		}
		else {
			return super.checkFillingFromClause(value);
		}
	}
	
	@Override
	protected void convertFromContainer(FromContainer fromContainer,
			SqlSelectExpression expression) throws SqlObjectException {
		if (fromContainer == null) {
			return;
		}
		
		StringBuilder from = new StringBuilder();
		StringBuilder where = new StringBuilder();
		
		for (SqlObject item: fromContainer) {
			FromClauseItem fromItem = (FromClauseItem) item; 
			if (from.length() > 0) {
				from.append(',');
			}
			from.append(convertFromClauseItem(fromItem, expression));
			
			if (isJoin(fromItem)) {
				expression.joinedAlias = fromItem.getAliasOrName();
				Join join = fromItem.getJoin();
				expression.joinType = join.joinType;
				String joiningCondition = convertConditions(join.getJoinOn(), expression);
				if (!StringUtils.isEmpty(joiningCondition)) {
					if (where.length() > 0) {
						where.append(" AND ");
					}
					where.append(joiningCondition);
				}
				expression.joinedAlias = "";
			}
			expression.where = where.toString();
			
			
		}
		expression.from = from.toString();
		
	}
	
	@Override
	protected String convertFromClauseItem(FromClauseItem fromClauseItem,
			SqlSelectExpression expression) throws SqlObjectException {
		return convertTableExpression(fromClauseItem.getTableExpr());
	}
	
	
	protected boolean willAddPlusInRoundBrackets(SqlObject expr, String joinedAlias, String exprStr) {
		return ((expr instanceof QualifiedField) && ((QualifiedField)expr).alias.equals(joinedAlias)) ||
			((joinedAlias + ".").indexOf(exprStr) >= 0);

	}
	  

	@Override
	protected String convertPredicateComparison(PredicateComparison predicate, SqlSelectExpression expression)
            throws SqlObjectException {
		ColumnExpression left = predicate.getLeft();
		ColumnExpression right = predicate.getRight();
		if ((left == null) || (right == null)) {
			return "";
		}
		String operation = comparisonToString(predicate.comparison);
		String leftStr = convertExpr(left);
		String rightStr = convertExpr(right);
		boolean addToLeft = false;
		boolean addToRight = false;
        /* учтем контекст операции сравнения - это может быть условие слияния таблиц,
		   в этом случае постараемся определить, используя переданный алиас присоединенной таблицы,
		   куда в сравнении поставить (+) */
		
		if (!StringUtils.isEmpty(expression.joinedAlias)) {
			switch (expression.joinType) {
				case LEFT:
					addToLeft = willAddPlusInRoundBrackets(left, expression.joinedAlias, leftStr);
					addToRight = willAddPlusInRoundBrackets(right, expression.joinedAlias, rightStr);
					Preconditions.checkState(addToLeft || addToRight, 
							"SqlObjectsConvertorOra.ConvertPredicateComparison: Не удалось добавить (+) в условие левого внешнего слияния \"%s%s%s\"",
							leftStr, operation, rightStr);
					break;
				case RIGHT:
					addToLeft = willAddPlusInRoundBrackets(left, expression.joinedAlias, rightStr);
					addToRight = willAddPlusInRoundBrackets(right, expression.joinedAlias, leftStr);
					Preconditions.checkState(addToLeft || addToRight, 
							"SqlObjectsConvertorOra.ConvertPredicateComparison: Не удалось добавить (+) в условие правого внешнего слияния \"%s%s%s\"",
							leftStr, operation, rightStr);
					break;
				case INNER:
					break;
				case FULL_OUTER:
					Preconditions.checkState(false,
							"SqlObjectsConvertorOra.ConvertPredicateComparison: Полные внешние соединения в Oracle не реализованы");
					break;
				default:
					Preconditions.checkState(false,
							"SqlObjectsConvertor.ConvertPredicateComparison: Неизвестный тип слияния таблиц");
					break;
				
			}
		}
		Preconditions.checkState(!(addToLeft && addToRight),
				    "SqlObjectsConvertorOra.ConvertPredicateComparison: (+) добавлен в оба условия слияния \"%s%s%s\"",
				    leftStr, operation, rightStr);
		
		StringBuilder result = new StringBuilder(leftStr);
		if (addToLeft) {
			result.append("(+)");
		}
		result.append(operation);
		result.append(rightStr);
		if (addToRight) {
			result.append("(+)");
		}
		return result.toString();
	}
	
	@Override
	protected void convertWhere(Conditions conditions, SqlSelectExpression sqlExpression)
            throws SqlObjectException {
		String conditionsStr = convertConditions(conditions, sqlExpression);
		if (StringUtils.isEmpty(sqlExpression.where)) {
			sqlExpression.where = conditionsStr;
		}
		else if (!StringUtils.isEmpty(conditionsStr)) {
			StringBuilder result = new StringBuilder();
			result.append('(');
			result.append(sqlExpression.where);
			result.append(") AND ");
			boolean addBrackets = conditions.booleanOp != Conditions.BooleanOp.AND;
			if (addBrackets) {
				result.append('(');
			}
			result.append(conditionsStr);
			if (addBrackets) {
				result.append(')');
			}
			sqlExpression.where = result.toString();
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

	private String convertFetchFirst(CursorSpecification cursor) {
		return "ROWNUM <= " + Long.toString(cursor.getFetchFirst());
	}

	private String convertFetchFirstAndWhere(CursorSpecification cursor, String sql_where) {
		if (!StringUtils.isEmpty(sql_where))
			return "(" + convertFetchFirst(cursor) + ") AND (" + sql_where + ")";
		else
			return convertFetchFirst(cursor);
	}

	@Override
	public String convertCursorSpecification(CursorSpecification cursor) {
		OrderBy orderBy = cursor.findOrderBy();
		SelectedColumnsContainer orgColumns, columns;
		Select select = cursor.getSelect();
		columns = select.getColumns();
		if (orderBy != null && cursor.getFetchFirst() > 0) {
			// если есть и сортировки и fetchfirst - в исходном запросе обеспечим уникальность имён колонок
			orgColumns = (SelectedColumnsContainer) columns.getClone();
			ensureUniqueColumnsAliases(columns);
		}
		else
			orgColumns = null;

		SqlSelectExpression sql = convertSelectClauses(select);
		if (orgColumns != null) {
			// есть и сортировки и fetchfirst - строим оборачивающий запрос
			String result =
					"SELECT * FROM(" +
							sql.asString(select.distinct, SqlObjectUtils.getRequestHint(select)) +
							getOrderByClause(cursor) +
							") WHERE " + convertFetchFirst(cursor);
			// восстановим исходные колонки, с исходными алиасами...
			select.setColumns(orgColumns);
			return result;
		}
		else {
			// есть что-то одно. или ничего.
			if (cursor.getFetchFirst() > 0)
				sql.where = convertFetchFirstAndWhere(cursor, sql.where);
			StringBuilder result = new StringBuilder();
			result.append(sql.asString(select.distinct, SqlObjectUtils.getRequestHint(select)));
			if (orderBy != null)
				result.append(getOrderByClause(cursor));
			return result.toString();
		}
	}
	

}
