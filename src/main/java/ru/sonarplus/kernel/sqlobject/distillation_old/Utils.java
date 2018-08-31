package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.DbSchemaSpec;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.dbschema.TableSpec;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedTableName;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.*;

public class Utils {
	protected static final List<CommonDistiller> DISTILLERS_LIST = getDistillersList();

	public Utils() {
	}
	
	protected static CommonDistiller findDistillerFor(SqlObject sqlObject) {
		for (CommonDistiller item: DISTILLERS_LIST) {
			if (item.isMy(sqlObject)) {
				return item;
			}
		}
		Preconditions.checkArgument(false, "Дистилляция объекта %s не поддержана",
				sqlObject.getClass().getSimpleName());
		return null;
	}
	
	protected static List<CommonDistiller> getDistillersList() {
		List<CommonDistiller> result = new ArrayList<CommonDistiller>();
		result.add(new AsteriskDistiller());
		result.add(new CaseSearchDistiller());
		result.add(new CaseSimpleDistiller());
		result.add(new ComparisonByRecordIdDistiller());
		result.add(new CTEDistiller());
		result.add(new DMLFieldAssignmentDistiller());
 		result.add(new ExpressionDistiller());
		result.add(new OrderByItemDistiller());
		result.add(new ParameterDistiller());
		result.add(new PredicateBetweenDistiller());
		result.add(new PredicateExistsDistiller());
		result.add(new PredicateForCodeComparisonDistiller());
		result.add(new PredicateFullTextSearchDistiller());
		result.add(new PredicateInSelectDistiller());
		result.add(new PredicateInTupleDistiller());
		result.add(new PredicateIsNullDistiller());
		result.add(new PredicateLikeDistiller());
		result.add(new PredicateRegExpMatchDistiller());
		result.add(new QualifiedFieldDistiller());
		result.add(new RecordIdColumnDistiller());
		result.add(new RecordIdInPredicateDistiller());
		result.add(new ScalarDistiller());
		result.add(new SelectedColumnDistiller());
		result.add(new SimpleComparisonDistiller());
		result.add(new SqlObjectsDistiller());
		result.add(new TupleExpressionsDistiller());
		result.add(new ValueDistiller());
		result.add(new ValueRecIdInPredicateDistiller());
		return result;
	}
	
	public static boolean objectClassIs(SqlObject sqlObject, Set<Class<? extends SqlObject>> classes) {
		for (Class<? extends SqlObject> classType: classes) {
			if (classType.isAssignableFrom(sqlObject.getClass())) {
				return true;
			}
		}
		return false;
	}

	public static boolean objectClassIs(SqlObject sqlObject, Class<? extends SqlObject>... classes) {
		for (Class<? extends SqlObject> classType: classes) {
			if (classType.isAssignableFrom(sqlObject.getClass())) {
				return true;
			}
		}
		return false;
	}
	
	public static SqlObject distillateSqlObject(SqlObject sqlObject,
			DbcRec dbc,
			NamesResolver namesResolver,
			DistillationParamsIn paramsIn,
			Set<Class<? extends SqlObject>> allowedClasses,
			Set<Class<? extends SqlObject>> ignorableClasses,
			Set<SqlObject> notDistillatedObjects,
			List<SqlObject> paramsForWrapWithExpr
			)
			throws Exception{
		Preconditions.checkNotNull(sqlObject, "Объект не задан");
		if (ignorableClasses != null) {
			if (objectClassIs(sqlObject, ignorableClasses)) {
				return null;
			}
		}
		if (allowedClasses != null) {
			Preconditions.checkArgument(objectClassIs(sqlObject, allowedClasses), 
					"Объект %s в данном случае не разрешён", sqlObject.getClass().getSimpleName());
		}
		return findDistillerFor(sqlObject).distillate(new DistillerState(sqlObject,
				dbc, namesResolver, paramsIn, notDistillatedObjects, paramsForWrapWithExpr));
		
		
	}
	
	public static SqlQuery distillateQueryEx(SqlQuery query,
			DbcRec dbc, boolean isNeedCloneAndRowRestriction,
			Set<SqlObject> notDistillatedObjects,
			List<SqlObject> paramsForWrapWithExpr	
			)
			throws Exception{
	
		distillateSqlObject(query, dbc, null, new DistillationParamsIn(isNeedCloneAndRowRestriction),
			null, null, notDistillatedObjects, paramsForWrapWithExpr);
		return query;
	}

	public static SqlQuery distillateQuery(SqlQuery query, DbcRec dbc)
			throws Exception{
		Set<SqlObject> notDistillatedObjects = new HashSet<SqlObject>();
		List<SqlObject> paramsForWrapWithExpr = new LinkedList<SqlObject>();
		distillateQueryEx(query, dbc, true, notDistillatedObjects, paramsForWrapWithExpr);
		return query;
	}
	
	public static String getPureAsteriskAlias(SqlObject source) {
		if (source instanceof Expression) {
			return ((Expression) source).getPureAsteriskAlias();
		}
		else {
			return null;
		}
	}

	public static ColumnExprTechInfo[] extractColumnsFromSubSelect(DbSchemaSpec schema, Select select)
			throws CloneNotSupportedException, SqlObjectException {
		Preconditions.checkNotNull(select);
		SubSelectColumnsExtractor extractor = new SubSelectColumnsExtractor(schema, select);
		return extractor.execute();
	}

	protected static ColumnExprTechInfo[] getFromTable(DbSchemaSpec schema, FromClauseItem fromItem) {
		TableSpec tableSpec = schema.findTableSpec(QualifiedTableName.stringToQualifiedTableName(((SourceTable)fromItem.getTableExpr().
				getSource()).table).table);
		Preconditions.checkNotNull(tableSpec);
		ColumnExprTechInfo[] result = new ColumnExprTechInfo[tableSpec.getFieldSpecCount()];
		for (int i = 0; i < result.length; i ++) {
			result[i] = ColumnExprTechInfo.createTechInfoByFieldSpec(tableSpec.getFieldSpec(i));
		}
		return result;
	}
	
	protected static ColumnExprTechInfo[] getFromCTE(CommonTableExpression cte)
			throws CloneNotSupportedException{
		ColumnExprTechInfo[] result = new ColumnExprTechInfo[cte.columns.size()];
		for (int i = 0; i < result.length; i++) {
			SelectedColumn column = (SelectedColumn) cte.getSelect().getColumns().getItem(i);
			result[i] = SqlObjectUtils.getTechInfo(column.getColExpr()).clone();
			result[i].dbdFieldName = cte.columns.get(i);
			result[i].nativeFieldName = result[i].dbdFieldName;
			result[i].resetIndexInfo();
		}
		return result;
		
	}

	
	protected static ColumnExprTechInfo[] extractColumnsFromTableOrCTE(DbSchemaSpec schema, FromClauseItem fromItem)
			throws CloneNotSupportedException, SqlObjectException {
		CommonTableExpression cte = SqlObjectUtils.CTE.findCTE(fromItem);
		if (cte != null) {
			return getFromCTE(cte);
		}
		else {
			return getFromTable(schema, fromItem);
		}
		
	}
	
	
	public static ColumnExprTechInfo[] extractColumns(DbSchemaSpec schema, FromClauseItem fromItem)
			throws SqlObjectException, CloneNotSupportedException {
		Source source = fromItem.getTableExpr().getSource();
		Preconditions.checkNotNull(source);
		if (source instanceof SourceQuery) {
			return extractColumnsFromSubSelect(schema, ((SourceQuery) source).findSelect());
		}
		else if (source instanceof SourceTable) {
			return extractColumnsFromTableOrCTE(schema, fromItem);
		}
		else {
			Preconditions.checkArgument(false, "extractColumns не реализован для класса "+source.getClass().getSimpleName());
			return null;
		}
	}
	
	public static ColumnExprTechInfo[] combineTechInfos(List<ColumnExprTechInfo[]> items) {
		int totalLength = 0;
		for (ColumnExprTechInfo[] listItem: items) {
			if (listItem != null) {
				totalLength += listItem.length; 
			}
		}
		
		ColumnExprTechInfo[] result = new ColumnExprTechInfo[totalLength];
		int index = 0;
		for (ColumnExprTechInfo[] listItem: items) {
			if (listItem != null) {
				for (ColumnExprTechInfo element: listItem) {
					result[index] = element;
					index++;
				}
			}
		}
		return result;
	}
	
	protected static void addCTEs(SqlObject target, SqlObject source)
			throws SqlObjectException {
		for (SqlObject item: source) {
			target.insertItem(item);
		}
	}
	
	protected static void iterateGetCTEs(SqlObject root, SqlObject ctes)
			throws SqlObjectException {
		for (SqlObject item: root) {
			if (!(item instanceof CTEsContainer)) {
				if (item instanceof Select) {
					CTEsContainer localCtes = getCTEs((Select) item);
					addCTEs(ctes, localCtes);
				}
				else {
					iterateGetCTEs(item, ctes);
				}
			}
		}
	}
	
	protected static void checkCTEs(CTEsContainer ctes) {
		CTEsChecker checker = new CTEsChecker();
		checker.execute(ctes);
	}
	
	protected static CTEsContainer getCTEs(Select select)
			throws SqlObjectException {
		CTEsContainer result = new CTEsContainer();
		CTEsContainer with = select.findWith();
		if (with != null) {
			while (with.isHasChilds()) {
				CommonTableExpression cte = (CommonTableExpression) with.firstSubItem();
				CTEsContainer subCtes = getCTEs(cte.getSelect());
				addCTEs(result, subCtes);
				result.insertItem(cte);
			}
		}
		iterateGetCTEs(select, result);
		checkCTEs(result);
		return result;
	}
	
	public static void prepareWithClausesForSelect(Select select)
			throws SqlObjectException {
		CTEsContainer ctes = getCTEs(select);
		CTEsContainer with;
		if (ctes.isHasChilds()) {
			with = select.newWith();
			while (ctes.isHasChilds()) {
				with.insertItem(ctes.firstSubItem());
			}
		}
		else {
			with = select.findWith();
			if (with != null) {
				select.setWith(null);
			}
		}
	}
	
	protected static void iterateWithClauses(SqlObject root)
			throws SqlObjectException {
		if (root instanceof Select) {
			prepareWithClausesForSelect((Select) root);
		}
		else {
			for (SqlObject item: root) {
				iterateWithClauses(item);
			}
		}
		
	}
	
	public static void prepareWithClauses(SqlQuery root)
			throws SqlObjectException {
		iterateWithClauses(root);
	}


	public static Select extractSelect(SqlQuery query) {
		if (query instanceof CursorSpecification) {
			return ((CursorSpecification) query).getSelect();
		}
		else if (query instanceof Select) {
			return (Select) query;
		}
		else {
			return null;
		}
		
	}
	
	public static void logNotDistillatedObjects(SqlQuery query, Set<SqlObject> notDistillated) {
		// TODO запротоколировать в log-файл
	}
	
	public static boolean isTechInfoFilled(ColumnExpression expr) {
		ColumnExprTechInfo techInfo = SqlObjectUtils.getTechInfo(expr);
		return techInfo != null && techInfo.techInfoPrepared;
	}

	public static FieldTypeId getFieldTypeId(ColumnExpression expr) {
		return Preconditions.checkNotNull(SqlObjectUtils.getTechInfo(expr).fieldTypeId);
	}
	
	public static boolean isCaseSensitive(ColumnExpression expr) {
		return SqlObjectUtils.getTechInfo(expr).caseSensitive;
	}
	
	public static boolean isQField(SqlObject source) {
		return (source instanceof QualifiedField) && (!(SqlObjectUtils.isAsterisk((QualifiedField) source) || SqlObjectUtils.isRecordId((QualifiedField)source)));
	}
	
	public static SqlQuery getParentQueryEx(SqlObject item) {
		if (item == null) {
			return null;
		}
		SqlQuery result = SqlObjectUtils.findParentQuery(item);
		if (result instanceof CursorSpecification) {
			result = ((CursorSpecification) result).findSelect();
		}
        // на всякий случай - если искали парента у верхнего запроса - вернём nil
		if (result == item) {
			result = null;
		}
		return result;
	}
	
	
	protected static boolean isHasRecIdOperand(PredicateComparison predicate) {
		return SqlObjectUtils.isRecordId(predicate.getLeft()) || SqlObjectUtils.isRecordId(predicate.getRight());
	}
	
	protected static boolean isParamValueRecId(SqlObject source, ColumnExpression expr) {
		Preconditions.checkArgument(!(expr instanceof ValueRecId), "Условие по RecId должно быть параметризовано");
		if (expr instanceof ParamRef) {
			SqlQuery root = SqlObjectUtils.getRootQuery(source);
			QueryParam queryParam = root.getParams().findParam(((Parameter) expr).parameterName);
			return queryParam.isRecId();
		}
		return false;
	}
	
	protected static boolean isHasParamValueRecIdOperand(SqlObject source, PredicateComparison predicate) {
		return isParamValueRecId(source, predicate.getLeft()) || isParamValueRecId(source, predicate.getRight());
	}
	
	
	public static boolean isByRecIdComparison(SqlObject source) {
		if (source instanceof PredicateComparison) {
			PredicateComparison cmp = (PredicateComparison) source;
			if (isHasRecIdOperand(cmp)) {
				boolean result = isHasParamValueRecIdOperand(source, cmp);
				Preconditions.checkArgument(result, 
						"В сравнении по RecId должны содержаться идентификатор записи и значение идентификатора записи.");
				return result;
			}
			else {
				Preconditions.checkArgument(!isHasParamValueRecIdOperand(source, cmp));
			}
		}
		return false;
	}
	
	public static boolean isSimpleComparison(SqlObject source) {
		return (source instanceof PredicateComparison) && ! isByRecIdComparison(source);
	}
	
	public static String getNextTechParamName(SqlObject item, String prefix) {
		QueryParams params = SqlObjectUtils.getRootQuery(item).getParams();
		int count = params != null ? params.itemsCount() : 0;
		return SqlObjectUtils.getTechParamPrefix(prefix) + count;
	}

    public static String getNextTechParamName(QueryParams params, String prefix) {
        int count = params != null ? params.itemsCount() : 0;
        return SqlObjectUtils.getTechParamPrefix(prefix) + count;
    }

}