package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.dbschema.FieldSpec;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.dbschema.TableSpec;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedTableName;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.*;

public class SqlObjectsDistiller extends CommonDistiller {
	protected static final Set<Class<? extends SqlObject>> DISTILLABLE_OBJECTS = getDistillableObjects();

	public SqlObjectsDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof SqlQuery;
	}
	
	protected boolean isRootQuery(SqlObject obj) {
		return obj == SqlObjectUtils.getRootQuery(obj);
	}
	
	protected void checkColumnsForSelect(SqlQuery query){
		Select select = Utils.extractSelect(query);
		if (select != null) {
			SelectedColumnsContainer container = select.getColumns();
			Preconditions.checkState((container != null) && (container.isHasChilds()),
					"В разделе SELECT должны быть перечислены выбранные колонки или указан QName('*')/QName('Алиас', '*')");
		}
		
	}
	
	protected void checkCteFirstUnitedSelectIsNotRecursive(CommonTableExpression cte) {
	    // первый запрос CTE, до Union, должен быть фиксированным - не рекурсивным, 
		// т.е. в его разделе From не должно быть ссылок на данный CTE
		FromContainer from = cte.getSelect().getFrom();
		if (from != null) {
			for (SqlObject item: from) {
				FromClauseItem fromItem = (FromClauseItem) item;
				Source source = fromItem.getTableExpr().getSource();
				if (source instanceof SourceTable) {
					Preconditions.checkState(!((SourceTable) source).table.equalsIgnoreCase(cte.alias), 
							"Первый запрос (до UNION) в CTE \"%s\" не должен быть рекурсивным",
							cte.alias);
				}
			}
		}
	}
	
	protected void distilateCTEs(SqlQuery query, DistillerState state)
			throws Exception{
		Select select = Utils.extractSelect(query);
		if (select == null) {
			return;
		}
		
		CTEsContainer with = select.findWith();
		if (!((with != null) && with.isHasChilds()))
			return;

		Set<SqlObject> notDistillated = new LinkedHashSet<SqlObject>();
		for (SqlObject item: with) {
			CommonTableExpression cte = (CommonTableExpression) item;
			checkCteFirstUnitedSelectIsNotRecursive(cte);
			Utils.distillateQueryEx(cte.getSelect(), state.dbc,
					false, notDistillated, state.paramsForWrapWithExpr);
		}
		if (!notDistillated.isEmpty()) {
			Utils.logNotDistillatedObjects(query, notDistillated);
			Preconditions.checkState(false,
					"При дистилляции подзапроса в разделе WITH остались недистиллированные объекты. См. протокол.");
		}
		
	}
	
	protected boolean isNeedToDistilate(SqlObject source, boolean isRoot) {
		if ((source instanceof Select) && !(isRoot || (source.getOwner() instanceof CursorSpecification))) {
			return true;
		}
		if ((source instanceof ColumnExpression) && Utils.isTechInfoFilled((ColumnExpression) source)) {
			return false;
		}
		return Utils.objectClassIs(source, DISTILLABLE_OBJECTS);
	}
	
	protected void iterateQuery(SqlObject root, boolean isRoot, List<SqlObject> objectsForDistilation) {
		if ((root instanceof QueryParams) || ((root instanceof Parameter) && (root.getOwner() instanceof QueryParams))) {
			return;
		}
		if (root instanceof FromContainer) {
			for (SqlObject item: root) {
				FromClauseItem fromItem = (FromClauseItem) item;
				Join join = fromItem.getJoin();
				if (join != null) {
					iterateQuery(join, false, objectsForDistilation);
				}
			}
			return;
		}
		else if ((root instanceof CTEsContainer) || (root instanceof CommonTableExpression)) {
			return;
		}
		else if (isNeedToDistilate(root, isRoot)) {
			objectsForDistilation.add(root);
		}
		else {
			for (SqlObject item: root) {
				iterateQuery(item, false, objectsForDistilation);
			}
			
		}
	}
	protected void internalPrepareNamesResolver(SqlQuery query, DistillerState state,
			Set<SqlObject> objectsFromJoinsNotDistilated)
			throws Exception{
		DistillerState lState = state.clone();
		lState.objectsNotDistilated = objectsFromJoinsNotDistilated;
		List<SqlObject> list = getFromClausesList(query, null);
		prepareNamesResolver(query, list, lState);
		state.namesResolver = lState.namesResolver; 
	}
	
	protected List<SqlObject> getFromClausesList(SqlObject root, List<SqlObject> list) {
		List<SqlObject> result = list != null ? list : new ArrayList<SqlObject>();
		if (root == null) {
			return result;
		}
		if (root instanceof CursorSpecification) {
			getFromClausesList(((CursorSpecification) root).getSelect(), result);
		}
		else if (root instanceof Select) {
			Select select = (Select) root;
			FromContainer from = select.findFrom();
			if (from != null) {
				result.add(from);
			}
			UnionsContainer unions = select.findUnions();
			getFromClausesList(unions, result);
		}
		else if (root instanceof UnionsContainer) {
			for (SqlObject item: root) {
				getFromClausesList(item, result);
			}
		}
		else if (root instanceof UnionItem) {
			getFromClausesList(((UnionItem) root).findSelect(), result);
		}
		return result;
		
	}
	
	protected void distillateList(List<SqlObject> objectsForDistillation,
			DistillerState state)
			throws Exception{
		for (SqlObject item: objectsForDistillation) {
			Utils.distillateSqlObject(item, state.dbc,
					state.namesResolver,
					new DistillationParamsIn(false),
					null, null, state.objectsNotDistilated,
					state.paramsForWrapWithExpr);
		}
	}
	
	public void distillateNotDistillated(DistillerState state)
			throws Exception{
		List<SqlObject> tmpList = new ArrayList<SqlObject>(state.objectsNotDistilated);
		state.objectsNotDistilated.clear();
		distillateList(tmpList, state);
	}
	
	public void distillateQuery(SqlQuery query, DistillerState state)
			throws Exception{
		checkColumnsForSelect(query);
		distilateCTEs(query, state);
		
	    // Начало "большой" дистилляции
	    // получить перечень sql-объектов, подлежащих дистилляции
		List<SqlObject> objectsForDistillation = new ArrayList<SqlObject>();
		iterateQuery(query, true, objectsForDistillation);

		//подготовить таблицу (карту) разрешения (сопоставления) имен полей, звездочек и идентификаторов записей
		Set<SqlObject> objectsFromJoinsNotDistilated = new LinkedHashSet<SqlObject>();
		internalPrepareNamesResolver(query, state, objectsFromJoinsNotDistilated);
	    //дистиляция всех отобранных объектов
		distillateList(objectsForDistillation, state);
		
	    // в процессе дистилляции подзапросов (например, в составе предикатов exist/in(select)) могут остаться
	    // недистилированные выражения (поля), по-причине того, что подзапрос в своем контексте не мог разрешить их имена,
	    // попробуем разрешить их здесь
		distillateNotDistillated(state);
		
	    // подзапросы в разделе FROM не могут смотреть на таблицы запросов верхних уровней.
	    //    "взрываемся", если остались недистилированные поля 
		if (objectsFromJoinsNotDistilated.size() > 0) {
			//TODO залогировать
			Preconditions.checkState(false,"В разделе FROM запроса остались необработанные объекты. "+
			"Возможно, из подзапросов в разделе FROM есть ссылки на табличные выражения запросов верхних уровней");
		}
		
		
		
	}
	
	protected void createParamWrappers(List<SqlObject> paramsForWrap, SqlQuery root, DistillerState state)
			throws CloneNotSupportedException, SqlObjectException {
		if (paramsForWrap != null) {
			for (SqlObject item: paramsForWrap) {
				Parameter param = (Parameter) item;
				SqlObject paramOwner = param.getOwner();
				ColumnExpression wrapper = state.dbc.dbSupport.createColumnExprForParameter(param, 
						root.getParams().getValueType(param.parameterName));
				SqlObjectUtils.buildTechInfoForExpr(wrapper, FieldTypeId.tid_UNKNOWN, true);
				paramOwner.replace(param, wrapper);
			}
		}
		
	}
	
	protected SqlQuery rootDistillate(DistillerState state, DistillerState localState)
			throws Exception{
		SqlQuery result = (SqlQuery) state.sqlObject;
		if (isRootQuery(result)) {
			localState.root = result;
		}
		Utils.prepareWithClauses(result);
		SqlObjectUtils.buildTreesForExpressions(result);
		this.distillateQuery(result, localState);
		Normalizer.normalize(result);
		createParamWrappers(localState.paramsForWrapWithExpr, result, state);
		return result;
		
	}
	
	protected SqlQuery localDistillation(DistillerState state)
			throws Exception{
		SqlQuery result = (SqlQuery) state.sqlObject;
		this.distillateQuery(result, state);
		return result;
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		DistillerState localState = state.clone();
		localState.namesResolver = null;
		if (localState.paramsIn.isNeedCloneAndRowRestrict) {
			return rootDistillate(state, localState);
		}
		else {
			return localDistillation(localState);
		}
	}
	
	public void prepareNamesResolver(SqlObject sqlObject, List<SqlObject> fromClauses, DistillerState state)
			throws Exception{
		state.namesResolver = new NamesResolver(state.dbc);
		ExtractorNamesForResolving extractor = new ExtractorNamesForResolving(sqlObject, this, state, fromClauses);
		extractor.execute();
	}
	
	public static class ExtractorNamesForResolving {
		protected SqlObjectsDistiller distiller;
		protected DistillerState state;
		protected SqlObject sqlObject;
		protected List<SqlObject> fromClauses;
		protected Set<String> names = new LinkedHashSet<String>();
		
		public ExtractorNamesForResolving(SqlObject sqlObject, SqlObjectsDistiller distiller,
				DistillerState state, List<SqlObject> fromClauses) {
			
			this.sqlObject = sqlObject;
			this.distiller = distiller;
			this.state = state;
			this.fromClauses = fromClauses;
			
		}
		
		protected void walk(SqlObject root) {
			for (SqlObject item: root) {
				if (item instanceof QualifiedField) {
					QualifiedField qField = (QualifiedField) item;
					names.add(QualifiedName.formQualifiedNameString(distiller.fieldAliasPart(qField), qField.fieldName));
				}
				else {
					walk(item);
				}
			}
		}
		
		public void scanForNames() {
			walk(sqlObject);
		}
		
		protected Set<String> createKeyList(String[] keys, Set<String> existingNames) {
			Set<String> result = new LinkedHashSet<String>();
			for (String item: keys) {
				if (existingNames.contains(item)) {
					result.add(item);
				}
			}
			return result;
		}
		
		public void filteredAddResolving(SqlQuery query, String[] keys, FieldSpec fieldSpec,
				FromClauseItem fromItem) {
			filteredAddResolving(query, keys, ColumnExprTechInfo.createTechInfoByFieldSpec(fieldSpec), fromItem);
		}
		
		public void filteredAddResolving(SqlQuery query, String[] keys, ColumnExprTechInfo fieldSpec,
				FromClauseItem fromItem) {
			Set<String> list = createKeyList(keys, names);
			for (String item: list) {
				state.namesResolver.addResolving(query, item, fieldSpec, fromItem);
			}
			
		}
		
		public void extractFromQuery(SqlQuery query, String alias, FromClauseItem fromItem)
				throws Exception{
			Utils.distillateQueryEx(((SourceQuery)fromItem.getTableExpr().getSource()).findSelect(),
					state.dbc, false, state.objectsNotDistilated, state.paramsForWrapWithExpr);
			ColumnExprTechInfo[] infoItems = Utils.extractColumns(state.dbc.schemaSpec, fromItem);
			for (ColumnExprTechInfo info: infoItems) {
				String[] fields = new String[]{info.dbdFieldName,
						QualifiedName.formQualifiedNameString(alias, info.dbdFieldName)};
				filteredAddResolving(query, fields, info, fromItem);
			}
			state.namesResolver.addAsterisk(query, alias, infoItems);
		}
		
		public static boolean isDistilableTable(String tableName, DbcRec dbc) {
			//TODO Результат функции должен быть СУБД-зависимым
			return !QualifiedTableName.stringToQualifiedTableName(tableName).table.equalsIgnoreCase("DUAL");
		}
		
		protected void addResolvingFields(SqlQuery query, FromClauseItem fromItem, TableSpec tableSpec, String tableAlias) {
			for (FieldSpec item: tableSpec.items) {
				filteredAddResolving(query, new String[] {item.getFieldName(), 
						QualifiedName.formQualifiedNameString(tableAlias, item.getFieldName())},
						item, fromItem);
			}
		}
		
		protected void addAsteriskFields(SqlQuery query, TableSpec tableSpec, String tableAlias) {
			state.namesResolver.addAsterisk(query, tableAlias, tableSpec);
		}
		
		protected void addRecordIdFields(SqlQuery query, TableSpec tableSpec, String tableAlias) {
			state.namesResolver.addRecordId(query, tableAlias, tableSpec);
		}
		
		public void extractFromTable(SqlQuery query, String tableName, String tableAlias, 
				boolean isFirstFromItem, boolean isNeedAsterisk, FromClauseItem fromItem) {
			if ((state.dbc.schemaSpec == null) || !isDistilableTable(tableName, state.dbc)) {
				return;
			}
			//TODO Это может быть временная таблица, которой нет в описателе, но которая используется в запросе
			TableSpec tableSpec = state.dbc.schemaSpec.findTableSpec(QualifiedTableName.stringToQualifiedTableName(tableName).table);
			if (tableSpec == null) {
				return;
			}
			String tableIndentifier = StringUtils.isEmpty(tableAlias) ? tableName : tableAlias;
			addRecordIdFields(query, tableSpec, tableIndentifier);
			if (isNeedAsterisk) {
				addAsteriskFields(query, tableSpec, tableIndentifier);
			}
			addResolvingFields(query, fromItem, tableSpec, tableIndentifier);
		}
		
		public void processDML() {
			state.namesResolver.createNamesSpace((SqlQuery)sqlObject);
			extractFromTable((SqlQuery)sqlObject, ((DataChangeSqlQuery)sqlObject).table, "",
					true, false, null);
		}
		
		public void extractFromCTE(SqlQuery query, String tableExprAlias, CommonTableExpression cte,
				FromClauseItem fromItem)
				throws SqlObjectException, CloneNotSupportedException {
			String cteAlias = StringUtils.isEmpty(tableExprAlias) ? cte.alias : tableExprAlias;

			// CTE-запрос уже должен быть дистиллирован
			ColumnExprTechInfo[] techInfos = Utils.extractColumns(state.dbc.schemaSpec, fromItem);
			
			for (ColumnExprTechInfo techInfoItem: techInfos) {
				filteredAddResolving(query, new String[]{techInfoItem.dbdFieldName,
						QualifiedName.formQualifiedNameString(cteAlias, techInfoItem.dbdFieldName)},
						techInfoItem, fromItem);
			}
			state.namesResolver.addAsterisk(query, cteAlias, techInfos);
			
		}
		
		public void execute()
				throws Exception{
			scanForNames();
			if ((sqlObject instanceof Select) || (sqlObject instanceof CursorSpecification)) {
				processSelect();
			}
			else if (sqlObject instanceof DataChangeSqlQuery) {
				processDML();
			}
		}
		
		public void processSelect()
				throws Exception{
			Select select;
			if (sqlObject instanceof CursorSpecification) {
				select = ((CursorSpecification) sqlObject).getSelect();
			}
			else {
				select = (Select) sqlObject;
			}
			boolean isFirstFromInClause = true;
			for (SqlObject item: fromClauses) {
				FromContainer fromClause = (FromContainer) item;  
				for (SqlObject fromElement:  fromClause) {
					FromClauseItem fromItem = (FromClauseItem) fromElement;
					SqlQuery parentQuery = SqlObjectUtils.getParentQuery(fromItem);
					state.namesResolver.createNamesSpace(parentQuery);
					Source source = fromItem.getTableExpr().getSource(); 
					if (source instanceof SourceTable) {
						CommonTableExpression cte = SqlObjectUtils.CTE.findCTE(fromItem.getTableName(), select);
						if (cte != null) {
							extractFromCTE(parentQuery, fromItem.getAlias(), cte, fromItem);
						}
						else {
							extractFromTable(parentQuery, fromItem.getTableName(), fromItem.getAlias(), isFirstFromInClause, true, fromItem);
						}
					}
					else if (source instanceof SourceQuery) {
						extractFromQuery(parentQuery, fromItem.getAlias(), fromItem);
					}
					else {
						Preconditions.checkArgument(false);
					}
					isFirstFromInClause = false;
				}
				
			}
			
		}
	}
	
	protected static Set<Class<? extends SqlObject>> getDistillableObjects() {
		Set<Class<? extends SqlObject>> result = new HashSet<Class<? extends SqlObject>>();
		result.add(CaseSearch.class);
		result.add(CaseSimple.class);
		result.add(DMLFieldAssignment.class);
		result.add(Expression.class);
		result.add(OraFTSMarker.class);
		result.add(OrderByItem.class);
		result.add(PredicateBetween.class);
		result.add(PredicateComparison.class);
		result.add(PredicateExists.class);
		result.add(PredicateForCodeComparison.class);
		result.add(PredicateFullTextSearch.class);
		result.add(PredicateIn.class);
		result.add(PredicateIsNull.class);
		result.add(PredicateLike.class);
		result.add(PredicateRegExpMatch.class);
		result.add(QualifiedField.class);
		result.add(Scalar.class);
		result.add(SelectedColumn.class);
		result.add(TupleExpressions.class);
		result.add(Value.class);
		result.add(ValueRecId.class);
		
		return result;
		
	}
	
	

}
