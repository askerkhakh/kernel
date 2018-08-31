package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;

import java.util.Set;

public abstract class PredicateInBaseDistiller extends CommonBaseDistiller {
	protected static final Set<Class<? extends SqlObject>> COLUMN_EXPRESSION = getAllowedClasses(ColumnExpression.class);
	protected static final Set<Class<? extends SqlObject>> PARAMETER_VALUE = getAllowedClasses(Parameter.class, Value.class);
	
	protected boolean tryDistillateTupleWithoutValues(TupleExpressions tuple, DistillerState state)
			throws Exception{
		boolean result = true;
		for (int i = 0; i < tuple.itemsCount(); i++) {
			ColumnExpression expr = (ColumnExpression) tuple.getItem(i);
			if (!Utils.objectClassIs(expr, Parameter.class, Value.class)) {
				if (!Utils.isTechInfoFilled(expr)) {
					internalDistillateObject(new DistillerState(expr, new DistillationParamsIn(FieldTypeId.tid_UNKNOWN), state),
							COLUMN_EXPRESSION, null);
				}
			}
		}
		for (SqlObject item: tuple) {
			ColumnExpression expr = (ColumnExpression) item;
			if (!Utils.objectClassIs(expr, Parameter.class, Value.class)) {
				result = result && Utils.isTechInfoFilled(expr);
			}
		}
		return result;
		
	}
	
	protected void distillateTupleParamsAndValues(TupleExpressions tuple, FieldTypeId type, DistillerState state)
			throws Exception{
		DistillationParamsIn paramsIn = new DistillationParamsIn(type);
		for (int i = 0; i < tuple.itemsCount(); i++) {
            SqlObject item = tuple.getItem(i);
			if (Utils.objectClassIs(item, Parameter.class, Value.class)) {
				internalDistillateObject(new DistillerState(item, paramsIn, state), PARAMETER_VALUE, null);
			}
		}
	}
}
