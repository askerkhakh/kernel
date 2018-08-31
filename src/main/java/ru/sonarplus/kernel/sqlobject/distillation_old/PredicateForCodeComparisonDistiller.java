package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.CodeValue;
import ru.sonarplus.kernel.sqlobject.db_support.PredicateForCodeComparisonTranslator;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.Set;

public class PredicateForCodeComparisonDistiller extends CommonBaseDistiller {
	protected static final Set<Class<? extends SqlObject>> QUALIFIED_FIELD = getAllowedClasses(QualifiedField.class);
	public PredicateForCodeComparisonDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof PredicateForCodeComparison;
	}
	
	protected boolean tryDistillateOperand(PredicateForCodeComparison predicate, DistillerState state)
            throws Exception{
		QualifiedField field = predicate.getField();
		Preconditions.checkNotNull(field);
		Preconditions.checkArgument( !isRecordId(field) && !SqlObjectUtils.isAsterisk(field),
				"В кодификаторном условии нельзя использовать RecordId или \"*\"")	;
		if (!Utils.isTechInfoFilled(field)) {
			internalDistillateObject(new DistillerState(field, state), QUALIFIED_FIELD, null);
		}
		return Utils.isTechInfoFilled(predicate.getField());
	}
	
	protected ColumnExprTechInfo getTechInfo(PredicateForCodeComparison predicate) {
		QualifiedField field = predicate.getField();
		ColumnExprTechInfo result = SqlObjectUtils.getTechInfo(field);
		Preconditions.checkState((result.fieldTypeId == FieldTypeId.tid_UNKNOWN) || 
				(result.fieldTypeId == FieldTypeId.tid_CODE),
				"Поле %s в условии %s должно иметь тип - \"%s\", получен - \"%s\"",
				field.getQName().qualifiedNameToString(), predicate.getClass().getSimpleName(),
				FieldTypeId.tid_CODE.toString(),
				result.fieldTypeId.toString());
		return result;		
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
            throws Exception{
		PredicateForCodeComparison predicateOrg = (PredicateForCodeComparison) state.sqlObject;
		if (tryDistillateOperand(predicateOrg, state)) {
			ColumnExpression code = predicateOrg.getCode();
			Preconditions.checkNotNull(code);
			ColumnExprTechInfo fieldTechInfo = SqlObjectUtils.getTechInfo(predicateOrg.getField());
			Preconditions.checkState(fieldTechInfo.fieldTypeId == FieldTypeId.tid_CODE ||
                    fieldTechInfo.fieldTypeId == FieldTypeId.tid_UNKNOWN,
                    "Поле %s в условии %s должно иметь тип - \"%s\", получен - \"%s\"",
                    predicateOrg.getField().getQName().qualifiedNameToString(),
                    predicateOrg.getClass().getSimpleName(), FieldTypeId.tid_CODE.toString(),
                    fieldTechInfo.fieldTypeId.toString());
			if (code instanceof ValueConst) {
				ValueConst codeConst = (ValueConst) code;
				Preconditions.checkState(codeConst.isNull() || (codeConst.getValueType() == FieldTypeId.tid_CODE),
						"В кодификаторном условии значение для сравнения должно быть типа \"%s\", получено \"%s\".",
						FieldTypeId.tid_CODE.toString(), codeConst.getValueType().toString());

                // Код кодификатора сделаем нужного размера
				byte[] codeValue= ((CodeValue) codeConst.getValue()).getValue();
                if (codeValue.length < fieldTechInfo.bytesForCode) {
                    byte[] newCodeValue = new byte[fieldTechInfo.bytesForCode];
                    for (int i = 0; i < codeValue.length; i++) {
                        newCodeValue[i] = codeValue[i];
                    }
                    codeConst.setValue(new CodeValue(newCodeValue));
                }

			}
			else {
				Preconditions.checkState(code instanceof ParamRef, 
						"Не реализована дистилляция для класса" +code.getClass().getSimpleName());
			}
			// создадим описатель для значения кода кодификатора
			SqlObjectUtils.buildTechInfoForExpr(code, FieldTypeId.tid_UNKNOWN, true);
            // обязательно выставим размер кода кодификатора. понадобится при вычислении диапазонов и проч..
			code.distTechInfo.bytesForCode = predicateOrg.getField().distTechInfo.bytesForCode;
			Predicate predicateNew = PredicateForCodeComparisonTranslator.translate(predicateOrg, state.dbc.dbSupport);
			replace(state.sqlObject, predicateNew);
			return predicateNew;
		}
		else {
			addNotDistillatedObject(predicateOrg, state);
			return null;
		}
	}

}
