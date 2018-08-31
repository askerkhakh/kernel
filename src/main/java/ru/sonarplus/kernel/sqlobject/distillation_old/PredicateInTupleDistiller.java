package ru.sonarplus.kernel.sqlobject.distillation_old;


import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;

public class PredicateInTupleDistiller extends PredicateInBaseDistiller {

	public PredicateInTupleDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof PredicateInTuple;
	}
	
	boolean tryDistillateExprWithoutValue(PredicateInTuple predicate, DistillerState state)
			throws Exception{
		ColumnExpression expr = predicate.getExpr();
		if (!Utils.objectClassIs(expr, Parameter.class, Value.class)) {
			if (!Utils.isTechInfoFilled(expr)) {
				return internalDistillateObject(new DistillerState(expr, new DistillationParamsIn(FieldTypeId.tid_UNKNOWN), state),
						COLUMN_EXPRESSION, null) != null;
			}
		}
		return true;
	}
	
	protected static class ExtractContext {
		FieldTypeId type;
		boolean isCs;
	}
	
	protected FieldTypeId getFieldTypeId(FieldTypeId first, FieldTypeId second,
			DistillerState state) {
		if (first == FieldTypeId.tid_UNKNOWN) {
			return second;
		}
		if (second == FieldTypeId.tid_UNKNOWN) {
			return first;
		}
		Preconditions.checkArgument(first == second, "В условии %s используются поля несовместимых типов: \"%s\"-\"%s\".",
				state.sqlObject.getClass().getSimpleName(), first.toString(), second.toString());
		return first;
	}
	
	protected void extractValueTypeAndCs(ColumnExpression expr, TupleExpressions tuple,
			DistillerState state,
			ExtractContext context) {
		context.type = FieldTypeId.tid_UNKNOWN;
		context.isCs = true;
		if (Utils.isTechInfoFilled(expr)) {
			context.type = getFieldTypeId(context.type, Utils.getFieldTypeId(expr), state);
			context.isCs = context.isCs && Utils.isCaseSensitive(expr);
		}
		for (SqlObject item: tuple) {
			ColumnExpression exprItem = (ColumnExpression) item;
			if (Utils.isTechInfoFilled(exprItem)) {
				context.type = getFieldTypeId(context.type, Utils.getFieldTypeId(exprItem), state);
				context.isCs = context.isCs && Utils.isCaseSensitive(exprItem);
				
			}
		}
	
	}
	
	protected void distillateExprParamAndValue(ColumnExpression expr, FieldTypeId type,
			DistillerState state)
			throws Exception{
		if (Utils.objectClassIs(expr, Parameter.class, Value.class)) {
			internalDistillateObject(new DistillerState(expr, new DistillationParamsIn(type), state),
					PARAMETER_VALUE, null);
		}
	}
	
	protected void wrapWithUpper(ColumnExpression expr, TupleExpressions tuple, boolean isCaseSensitive,
			DistillerState state)
			throws SqlObjectException {
		wrapWithUpper(expr, isCaseSensitive, state);
		for (SqlObject item: expr) {
			wrapWithUpper((ColumnExpression)item, isCaseSensitive, state);
		}
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		PredicateInTuple predicate = (PredicateInTuple) state.sqlObject;
		if (tryDistillateExprWithoutValue(predicate, state) && tryDistillateTupleWithoutValues(predicate.getTuple(), state)) {
			ColumnExpression expr = predicate.getExpr();
			TupleExpressions tuple = predicate.getTuple();
			ExtractContext context = new ExtractContext();
			extractValueTypeAndCs(expr, tuple, state, context);
			distillateTupleParamsAndValues(tuple, context.type, state);
			distillateExprParamAndValue(expr, context.type, state);
			wrapWithUpper(expr, tuple, context.isCs, state);
		}
		else {
			addNotDistillatedObject(predicate, state);
		}
		return predicate;
	}

}
