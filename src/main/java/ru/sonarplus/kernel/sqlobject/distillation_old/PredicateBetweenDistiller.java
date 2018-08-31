package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.Set;

public class PredicateBetweenDistiller extends CommonBaseDistiller {
	protected static final Set<Class<? extends SqlObject>> COLUMN_EXPRESSION = getAllowedClasses(ColumnExpression.class);
	protected static final Set<Class<? extends SqlObject>> VALUE_PARAMETER = getAllowedClasses(Value.class, Parameter.class);

	public PredicateBetweenDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof PredicateBetween;
	}
	
	protected boolean isParameterOrValue(SqlObject sqlObject) {
		return (sqlObject instanceof Parameter) || (sqlObject instanceof Value);
	}
	
	protected boolean tryDistilateOperands(DistillerState state, ColumnExpression operand)
			throws Exception{
		if (!isParameterOrValue(checkNotRecordId(operand, state))) {
			if (!Utils.isTechInfoFilled(operand)) {
				internalDistillateObject(new DistillerState(operand, state), COLUMN_EXPRESSION, null);
				return Utils.isTechInfoFilled(operand);
			}
		}
		return true;
	}
	
	
	protected boolean tryDistilateOperandsWithoutValues(DistillerState state, PredicateBetween predicate)
			throws Exception{
		return tryDistilateOperands(state, predicate.getExpr()) & tryDistilateOperands(state, predicate.getLeft()) &
				tryDistilateOperands(state, predicate.getRight());
	}
	
	protected FieldTypeId getFieldTypeId(FieldTypeId first, FieldTypeId second) {
		if (first == FieldTypeId.tid_UNKNOWN) {
			return second;
		}
		if (second == FieldTypeId.tid_UNKNOWN) {
			return first;
		}
		Preconditions.checkArgument(first == second, "В условии сравнения несовместимые типы \"%s\"-\"%s\"",
				first.toString(), second.toString());
		return first;
	}
	
	protected static class ExtractContext {
		public FieldTypeId valueType;
		public boolean isCs; 
	}
	
	protected void extractValueTypeAndCSByOperands(ExtractContext context, ColumnExpression... items) {
		for (ColumnExpression operand: items) { 
			if (!isParameterOrValue(operand)) {
				ColumnExprTechInfo techInfo = SqlObjectUtils.getTechInfo(operand);
				context.valueType = getFieldTypeId(context.valueType, techInfo.fieldTypeId);
				context.isCs = context.isCs &&  techInfo.caseSensitive;
			}
		}
	}
	
	protected void extractValueTypeAndCS(PredicateBetween predicate, ExtractContext context) {
		context.valueType = FieldTypeId.tid_UNKNOWN;
		context.isCs = true;
		extractValueTypeAndCSByOperands(context, predicate.getExpr(), predicate.getLeft(), predicate.getRight());
	}
	
	protected void distillateValues(DistillerState state, FieldTypeId valueType)
			throws Exception{
		DistillationParamsIn paramsIn = new DistillationParamsIn(valueType);
		for(SqlObject item: state.sqlObject)
            if (isParameterOrValue(item)) {
                internalDistillateObject(new DistillerState(item, paramsIn, state), VALUE_PARAMETER, null);
            }
	}
	
	protected void wrapWithUpper(DistillerState state, PredicateBetween predicate, boolean isCaseSensitive)
			throws SqlObjectException {
		wrapWithUpper(predicate.getExpr(), isCaseSensitive, state);
		wrapWithUpper(predicate.getLeft(), isCaseSensitive, state);
		wrapWithUpper(predicate.getRight(), isCaseSensitive, state);
	}

	

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		PredicateBetween predicate = (PredicateBetween) state.sqlObject;
		if (tryDistilateOperandsWithoutValues(state, predicate)) {
			ExtractContext context = new ExtractContext();
			extractValueTypeAndCS(predicate, context);
			distillateValues(state, context.valueType);
			wrapWithUpper(state, predicate, context.isCs);
		}
		else {
			addNotDistillatedObject(predicate, state);
		}
		return predicate;
	}

}
