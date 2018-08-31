package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;
import ru.sonarplus.kernel.sqlobject.objects.ParamRef;
import ru.sonarplus.kernel.sqlobject.objects.Parameter;
import ru.sonarplus.kernel.sqlobject.objects.QueryParam;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public class ParameterDistiller extends CommonBaseDistiller {

	public ParameterDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return (source != null) && (source instanceof ParamRef);
	}
	
	protected FieldTypeId calcValueType(DistillerState state, Parameter parameter, FieldTypeId valueType)
            throws  ValuesSupport.ValueException {
		
		QueryParam queryParam = state.root.getParams().findExistingParam(parameter.parameterName);
		if (valueType == FieldTypeId.tid_UNKNOWN)
            // тип значения выражения, сравниваемого с параметром, неизвестен. Возьмём тип значения параметра
			return queryParam.getValueType();
		else
			queryParam.setValueType(valueType);

		return valueType;
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws CloneNotSupportedException, ValuesSupport.ValueException {
		ParamRef result = (ParamRef) state.sqlObject;
		FieldTypeId valueType = calcValueType(state, result, state.paramsIn.valueType);
		state.paramsForWrapWithExpr.add(result);
		SqlObjectUtils.buildTechInfoForExpr(result, valueType, true);
		return result;
	}

}
