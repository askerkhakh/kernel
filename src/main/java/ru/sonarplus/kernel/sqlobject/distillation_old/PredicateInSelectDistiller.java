package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public class PredicateInSelectDistiller extends PredicateInBaseDistiller {

	public PredicateInSelectDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof PredicateInQuery;
	}
	
	protected boolean tryDistillateSubSelectColumnsWithoutValues(Select select, DistillerState state)
			throws Exception{
		Utils.distillateQueryEx(select, state.dbc, false, state.objectsNotDistilated, state.paramsForWrapWithExpr);
	    // описатели из выражений колонок
		SelectedColumnsContainer columns = select.getColumns();
		boolean result = true;
		for (SqlObject item: columns) {
			SelectedColumn columnItem = (SelectedColumn) item;
			ColumnExpression expr = columnItem.getColExpr();
			result = result && Utils.isTechInfoFilled(expr);
		}
		return result;
	}
	
	protected void checkTypeCompatibility(FieldTypeId first, FieldTypeId second, DistillerState state) {
		Preconditions.checkArgument((first == second) || (first == FieldTypeId.tid_UNKNOWN) || 
				(second == FieldTypeId.tid_UNKNOWN), 
				"В условии %s несовместимые типы значения из кортежа и соответствующего значения из колонки подзапроса (\"%s\"-\"%s\")",
				state.sqlObject.getClass().getSimpleName(),
				first.toString(), second.toString());
	}
	
	protected void justifyTypesAndCs(TupleExpressions tuple, Select select, DistillerState state)
			throws Exception{
		SelectedColumnsContainer columns = select.getColumns();
		for (int i = 0; i < tuple.itemsCount(); i++) {
			ColumnExpression tupleItem = (ColumnExpression) tuple.getItem(i);
			ColumnExpression selectItem = ((SelectedColumn)columns.getItem(i)).getColExpr();
			if (!(Utils.objectClassIs(tupleItem, Value.class, Parameter.class) ||
					Utils.objectClassIs(selectItem, Value.class, Parameter.class))) {
				checkTypeCompatibility(SqlObjectUtils.getTechInfo(tupleItem).fieldTypeId, SqlObjectUtils.getTechInfo(selectItem).fieldTypeId, state);
				boolean isCs = Utils.isCaseSensitive(tupleItem) && Utils.isCaseSensitive(selectItem);
				wrapWithUpper(tupleItem, isCs, state);
				wrapWithUpper(selectItem, isCs, state);
			}
			else {
				if (Utils.objectClassIs(tupleItem, Value.class, Parameter.class)) {
					if (Utils.objectClassIs(selectItem, Value.class, Parameter.class)) {
		                  // оба выражения - значения. тип приводим по первому.
						internalDistillateObject(new DistillerState(tupleItem,
								new DistillationParamsIn(FieldTypeId.tid_UNKNOWN), state),
						COLUMN_EXPRESSION, null);
						
						internalDistillateObject(new DistillerState(selectItem,
								new DistillationParamsIn(Utils.getFieldTypeId(tupleItem)), state),
						COLUMN_EXPRESSION, null);
					}
					else {
		                  // первое выражение - значение, второе уже дистиллированное, имеющее описатель
						internalDistillateObject(new DistillerState(tupleItem,
								new DistillationParamsIn(Utils.getFieldTypeId(selectItem)), state),
						COLUMN_EXPRESSION, null);
						boolean isCs = Utils.isCaseSensitive(selectItem);
						wrapWithUpper(tupleItem, isCs, state);
						wrapWithUpper(selectItem, isCs, state);
						
					}
				}
				else if (Utils.objectClassIs(selectItem, Value.class, Parameter.class)) {
					internalDistillateObject(new DistillerState(selectItem,
							new DistillationParamsIn(Utils.getFieldTypeId(tupleItem)), state),
					COLUMN_EXPRESSION, null);
					boolean isCs = Utils.isCaseSensitive(tupleItem);
					wrapWithUpper(tupleItem, isCs, state);
					wrapWithUpper(selectItem, isCs, state);
					
				}
			}
		}
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		PredicateInQuery predicate = (PredicateInQuery) state.sqlObject;
		Select select = Preconditions.checkNotNull(predicate.findSelect());
		TupleExpressions tuple = predicate.getTuple();
		if (!tryDistillateTupleWithoutValues(tuple, state)) {
			addNotDistillatedObject(predicate, state);
		}
		else if (select != null) {
			if (tryDistillateSubSelectColumnsWithoutValues(select, state)) {
				Preconditions.checkState(tuple.itemsCount() == select.getColumns().itemsCount());
				// дистиллируем оставшиеся значения-параметры, проверяем типы и применяем регистронезависимость
				justifyTypesAndCs(tuple, select, state);
			}
			else {
				addNotDistillatedObject(predicate, state);
			}
		}
		return predicate;
	}

}
