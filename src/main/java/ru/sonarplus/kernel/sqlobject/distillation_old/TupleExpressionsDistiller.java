package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.TupleExpressions;

import java.util.HashSet;
import java.util.Set;

public class TupleExpressionsDistiller extends CommonBaseDistiller {
	protected static final Set<Class<? extends SqlObject>> ALLOWED_COLUMN_EXPRESSION = getAllowedClasses();

	public TupleExpressionsDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof TupleExpressions;
	}
	
    protected boolean tryDistillateTuple(TupleExpressions tuple, DistillerState state)
			throws Exception{
        DistillationParamsIn paramsIn = new DistillationParamsIn(FieldTypeId.tid_UNKNOWN);
        for (int i = 0; i < tuple.itemsCount(); i++) {
            ColumnExpression expr = (ColumnExpression) tuple.getItem(i);
            if (!Utils.isTechInfoFilled(expr))
                internalDistillateObject(new DistillerState(expr, paramsIn, state), ALLOWED_COLUMN_EXPRESSION, null);
        }

		for (SqlObject item: tuple) {
			ColumnExpression expr = (ColumnExpression) item;
			if (!Utils.isTechInfoFilled(expr))
				return false;
		}
		return true;
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		TupleExpressions tuple = (TupleExpressions)state.sqlObject;
		if (!tryDistillateTuple(tuple, state)) {
			addNotDistillatedObject(tuple, state);
		}
		return tuple;
	}
	
	protected static Set<Class<? extends SqlObject>> getAllowedClasses() {
		Set<Class<? extends SqlObject>> result = new HashSet<Class<? extends SqlObject>>();
		result.add(ColumnExpression.class);
		return result;
	}

}
