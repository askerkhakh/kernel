package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.Value;
import ru.sonarplus.kernel.sqlobject.objects.ValueConst;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public class ValueDistiller extends CommonBaseDistiller {

	public ValueDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof ValueConst;
	  /* а есть ещё значения - ссылки на источники данных, 
		    ссылки на параметры узлов отчётов (и что-то ещё, что появится потом).
		    такие объекты  отрабатываются на этапе подготовки запроса */
		
	}
	protected static class ValueDistillerContext {
		public FieldTypeId resultType;
	}
	
	protected ColumnExpression prepareValue(DistillerState state, ValueDistillerContext context)
			throws SqlObjectException {
		context.resultType = FieldTypeId.tid_UNKNOWN;
		Value value = (Value) state.sqlObject;
		FieldTypeId neededType = value.getValueType();
		if ((state.paramsIn.valueType != FieldTypeId.tid_UNKNOWN) && (value.getValueType() != state.paramsIn.valueType)) {
			neededType = state.paramsIn.valueType;
		}
		context.resultType = neededType;
		return state.dbc.dbSupport.createColumnExprForValue(value.getValue(), neededType);
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws CloneNotSupportedException, SqlObjectException {
		ValueDistillerContext context = new ValueDistillerContext();
		SqlObject result = prepareValue(state,  context);
        ((ColumnExpression)result).distTechInfo = SqlObjectUtils.buildTechInfoForExpr((ColumnExpression) result, context.resultType, true);
		replace(state.sqlObject, result);
		return result;
	}

}
