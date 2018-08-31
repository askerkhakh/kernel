package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.Set;

public class SimpleComparisonDistiller extends CommonBaseDistiller {
	protected static final Set<Class<? extends SqlObject>> COLUMN_EXPRESSION = getAllowedClasses(ColumnExpression.class);
	protected static final Set<Class<? extends SqlObject>> VALUE_PARAMETER = getAllowedClasses(Value.class, Parameter.class);

	public SimpleComparisonDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return Utils.isSimpleComparison(source);
	}

	protected boolean isParameterOrValue(SqlObject sqlObject) {
		return (sqlObject instanceof Parameter) || (sqlObject instanceof Value);
	}
	
	protected boolean tryDistilateOperand(DistillerState state, ColumnExpression operand)
			throws Exception{
		if (!isParameterOrValue(operand)) {
			if (!Utils.isTechInfoFilled(operand)) {
				ColumnExpression distillatedOperand = (ColumnExpression) internalDistillateObject( new DistillerState(operand, state), COLUMN_EXPRESSION, null);
				return (distillatedOperand != null) && Utils.isTechInfoFilled(distillatedOperand);
			}
		}
		return true;
		
	}
	
	protected void checkTypes(FieldTypeId first, FieldTypeId second) {
		if ((first == FieldTypeId.tid_UNKNOWN) || (second == FieldTypeId.tid_UNKNOWN)) {
			return;
		}
		Preconditions.checkArgument(first == second, "В условии сравнения несовместимые типы \"%s\"-\"%s\"",
				first.toString(), second.toString());
	}
	
	protected FieldTypeId getType(ColumnExprTechInfo info, FieldTypeId defaultType) {
		return (defaultType == FieldTypeId.tid_UNKNOWN) && info.techInfoPrepared ? info.fieldTypeId : defaultType;
	}
	
	protected FieldTypeId getType(ColumnExprTechInfo info) {
		return getType(info, FieldTypeId.tid_UNKNOWN);
	}
	
	protected boolean tryDistilatePredicateOperands(DistillerState state, PredicateComparison predicate)
			throws Exception{
		return tryDistilateOperand(state, predicate.getLeft()) & tryDistilateOperand(state, predicate.getRight()); 
	}
	
	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		PredicateComparison predicate = (PredicateComparison) state.sqlObject;
		boolean isCS;
		if (tryDistilatePredicateOperands(state, predicate)) {
			ColumnExprTechInfo leftTechInfo = SqlObjectUtils.getTechInfo(predicate.getLeft());
			ColumnExprTechInfo rightTechInfo = SqlObjectUtils.getTechInfo(predicate.getRight());
			if (leftTechInfo.techInfoPrepared) {
				if (rightTechInfo.techInfoPrepared) {
		            // слева и справа - выражения
					checkTypes(getType(leftTechInfo), getType(rightTechInfo));
					isCS = leftTechInfo.caseSensitive && rightTechInfo.caseSensitive;
					wrapWithUpper(predicate.getLeft(), isCS, state);
					wrapWithUpper(predicate.getRight(), isCS, state);
				}
				else {
					internalDistillateObject(new DistillerState(predicate.getRight(),
							new DistillationParamsIn(getType(leftTechInfo)), state),
							VALUE_PARAMETER, null);
					isCS = leftTechInfo.caseSensitive;
					wrapWithUpper(predicate.getLeft(), isCS, state);
					wrapWithUpper(predicate.getRight(), isCS, state);
				}
			}
			else {
				if (rightTechInfo.techInfoPrepared) {
		              // слева - значение, справа - выражение
					internalDistillateObject(new DistillerState(predicate.getLeft(),
							new DistillationParamsIn(getType(rightTechInfo)), state),
							VALUE_PARAMETER, null);
					isCS = rightTechInfo.caseSensitive;
					wrapWithUpper(predicate.getLeft(), isCS, state);
					wrapWithUpper(predicate.getRight(), isCS, state);
				}
				else {
		              // слева и справа - значения
					internalDistillateObject(new DistillerState(predicate.getLeft(), state), VALUE_PARAMETER, null);
					internalDistillateObject(new DistillerState(predicate.getRight(), new DistillationParamsIn(getType(leftTechInfo)), state),
							VALUE_PARAMETER, null); 
				}
			}
		}
		else {
			addNotDistillatedObject(predicate, state);
		}
		return predicate;
	}

}
