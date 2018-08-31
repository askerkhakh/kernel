package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.RecIdValueWrapper;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExprTechInfo;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.Conditions;
import ru.sonarplus.kernel.sqlobject.objects.ParamRef;
import ru.sonarplus.kernel.sqlobject.objects.Parameter;
import ru.sonarplus.kernel.sqlobject.objects.Predicate;
import ru.sonarplus.kernel.sqlobject.objects.PredicateComparison;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedField;
import ru.sonarplus.kernel.sqlobject.objects.QueryParam;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.Value;
import ru.sonarplus.kernel.sqlobject.objects.ValueConst;
import ru.sonarplus.kernel.sqlobject.objects.ValueRecId;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.ArrayList;
import java.util.List;

public class ComparisonByRecordIdDistiller extends CommonBaseDistiller {

	public ComparisonByRecordIdDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return Utils.isByRecIdComparison(source);
	}
	
	protected static class OperandsContext {
		public QualifiedField  recId;
		public ValueRecId recIdValue;
		public Parameter orgQParam;
	}
	
	protected void getOperands(DistillerState state, PredicateComparison predicate, OperandsContext context) {
		Parameter paramRecId = null;
		ColumnExpression left = predicate.getLeft();
		ColumnExpression right = predicate.getRight();
		if (left instanceof Parameter) {
			paramRecId = (Parameter)left;
		}
		else if (right instanceof Parameter) {
			paramRecId = (Parameter)right;
		}
		Preconditions.checkNotNull(paramRecId);
		context.recId = null;
		if (left instanceof QualifiedField) {
			context.recId = (QualifiedField) left;
		}
		else if (right instanceof QualifiedField) {
			context.recId = (QualifiedField) right;
		}
		Preconditions.checkNotNull(context.recId);
		context.orgQParam = Preconditions.checkNotNull(state.root.getParams().findParam(paramRecId.parameterName));
		context.recIdValue = Preconditions.checkNotNull( (ValueRecId) ((QueryParam)context.orgQParam).getValueObj());
	}
	
	protected void checkRecId(ColumnExprTechInfo[] recordId, ValueRecId value) {
		Preconditions.checkNotNull(recordId);
		Preconditions.checkArgument(value.isNull() || (recordId.length == value.getRecId().count()));
	}
	
	protected Object getOneValue(int valueIndex, RecIdValueWrapper wrapper) {
		return wrapper.getValue(wrapper.isComposite() ? valueIndex : 0);
	}
	
	protected ParamRef createTechParam(String paramNamePrefix, Object value, FieldTypeId type,
			DistillerState state, List<Parameter> paramsList)
            throws SqlObjectException {
		Value valueItem = new ValueConst(value, type);
		QueryParam queryParam = new QueryParam(state.root.newParams(),
				Utils.getNextTechParamName(state.sqlObject, paramNamePrefix), valueItem, QueryParam.ParamType.INPUT);
		ParamRef result = new ParamRef(queryParam.parameterName);
		paramsList.add(result);
		return result;
	}
	
	protected Predicate buildConditionByOneField(String tableAlias,
			String latinFieldName, String paramNamePrefix, 
			Object value, FieldTypeId fieldTypeId,
			DistillerState state,
			List<Parameter> paramsList)
			throws SqlObjectException {
		QualifiedField qField = new QualifiedField(tableAlias, latinFieldName);
		Parameter techParam = createTechParam(paramNamePrefix, value, fieldTypeId, state, paramsList);
		return new PredicateComparison(qField, techParam, PredicateComparison.ComparisonOperation.EQUAL);
	}
	
	protected Predicate getResultPredicate(Conditions conditions)
			throws SqlObjectException {
		Predicate result;
		if (conditions.itemsCount() == 1) {
			result = (PredicateComparison) conditions.firstSubItem();
			conditions.removeItem(result);
			if (conditions.not) {
				((PredicateComparison) result).comparison = PredicateComparison.ComparisonOperation.NOT_EQUAL;
			}
		}
		else {
			return conditions;
		}
		return result;
	}
	
	protected void distillateCreatedParameters(List<Parameter> paramsList, DistillerState state)
			throws Exception{
		for (Parameter parameter: paramsList) {
			internalDistillateObject(new DistillerState(parameter, new DistillationParamsIn(FieldTypeId.tid_UNKNOWN),
					state), null, null);
			
		}
	}


	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		PredicateComparison predicate = (PredicateComparison) state.sqlObject;
		// сравнение по rec_id может быть только =/<>
		Preconditions.checkArgument((predicate.comparison == PredicateComparison.ComparisonOperation.EQUAL) ||
				(predicate.comparison == PredicateComparison.ComparisonOperation.NOT_EQUAL));
		OperandsContext context = new OperandsContext();
		getOperands(state, predicate, context);
		String tableAlias = fieldAliasPart(context.recId);
		ColumnExprTechInfo[] recordId = state.namesResolver.recordIdFields(SqlObjectUtils.getParentQuery(state.sqlObject), tableAlias);
		checkRecId(recordId, context.recIdValue);

		Conditions conditions = new Conditions(Conditions.BooleanOp.AND)
                // Not
                .setNot(
                        ((predicate.comparison == PredicateComparison.ComparisonOperation.EQUAL) && predicate.not) ||
                                ((predicate.comparison == PredicateComparison.ComparisonOperation.NOT_EQUAL) && (!predicate.not))
                );

        // на каждое поле, составляющее идентификатор записи, создадим параметризованное сравнение
		int i = 0;
		List<Parameter> paramsList = new ArrayList<Parameter>();
		for (ColumnExprTechInfo fieldSpec: recordId) {
			Predicate cmpByField = buildConditionByOneField(tableAlias, fieldSpec.nativeFieldName, 
					context.orgQParam.parameterName,
					getOneValue(i, context.recIdValue.getRecId()),
					fieldSpec.fieldTypeId,
					state,
					paramsList);
			conditions.addCondition(cmpByField);
			i++;
		}
		SqlObject result = getResultPredicate(conditions); 
		replace(predicate, result);
		
		// каждый созданный параметр обернём, при необходимости, СУБД-зависимым выражением
		distillateCreatedParameters(paramsList, state);
		return result;
	}

}
