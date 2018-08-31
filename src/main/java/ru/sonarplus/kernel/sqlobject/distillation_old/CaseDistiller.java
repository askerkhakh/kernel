package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.objects.Case.WhenThen;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.Set;

public abstract class CaseDistiller extends CommonBaseDistiller {
	protected static final Set<Class<? extends SqlObject>> VALUE_OR_PARAM = getAllowedClasses(Value.class, Parameter.class);
	protected static final Set<Class<? extends SqlObject>> COLUMN_EXPRESSION = getAllowedClasses(ColumnExpression.class);

	public CaseDistiller() {

	}

	
	protected void checkTypes(FieldTypeId first, FieldTypeId second, String clause, DistillerState state) {
		if ((first == FieldTypeId.tid_UNKNOWN) || (second == FieldTypeId.tid_UNKNOWN)) {
			return;
		}
		Preconditions.checkArgument(first == second, 
				"В конструкции %s, в разделах %s обнаружены несовместимые типы \"%s\"-\"%s\"",
				state.sqlObject.getClass().getSimpleName(), clause, first.toString(), second.toString());
			
	}
	
	protected boolean isDistillated(ColumnExpression expr, DistillerState state)
			throws Exception{
		if (Utils.isTechInfoFilled(expr)) {
			return true;
		}
		ColumnExpression distilatedExpr = (ColumnExpression) internalDistillateObject(new DistillerState(expr, state),
				COLUMN_EXPRESSION, null);
		return Utils.isTechInfoFilled(distilatedExpr);
	}
	
	protected FieldTypeId getType(ColumnExprTechInfo info, FieldTypeId defaultType) {
		return defaultType == FieldTypeId.tid_UNKNOWN ? info.fieldTypeId : defaultType;
	}
	
	protected Boolean getIsCS(Boolean orgCS, ColumnExprTechInfo info) {
		return orgCS == null ? info.caseSensitive : orgCS;
	}
	
	protected ColumnExprTechInfo getValueTypeForThenElseClause(Case caseItem, DistillerState state) {
		FieldTypeId fieldType = FieldTypeId.tid_UNKNOWN;
		Boolean isCaseSensitive = null;
		ColumnExprTechInfo fieldTechInfoPrev = new ColumnExprTechInfo();
		for (SqlObject item: caseItem) {
			if (item instanceof WhenThen) {
				WhenThen itemWhenThen = (WhenThen) item;
				ColumnExprTechInfo fieldTechInfo = SqlObjectUtils.getTechInfo(itemWhenThen.getThen());
				if (fieldTechInfo.techInfoPrepared) {
					isCaseSensitive = getIsCS(isCaseSensitive, fieldTechInfo);
					checkTypes(fieldTechInfoPrev.fieldTypeId, fieldTechInfo.fieldTypeId, "THEN", state);
					fieldTechInfoPrev = fieldTechInfo;
					fieldType = getType(fieldTechInfoPrev, fieldType);
				}
			}
		}
		ColumnExpression elseItem = caseItem.getElse(); 
		if (elseItem != null) {
			ColumnExprTechInfo fieldTechInfo = SqlObjectUtils.getTechInfo(elseItem);
			checkTypes(fieldTechInfoPrev.fieldTypeId, fieldTechInfo.fieldTypeId, "THEN/ELSE", state);
			fieldType = getType(fieldTechInfoPrev, fieldType);
		}
		ColumnExprTechInfo result = new ColumnExprTechInfo();
		result.fieldTypeId = fieldType;
		result.caseSensitive = (isCaseSensitive == null) || isCaseSensitive;
		return result;
	}
	
	protected boolean tryDistilateThenElseClauseWithoutValues(Case caseItem, DistillerState state)
			throws Exception{
		boolean result = true;
		ColumnExpression elseItem = caseItem.getElse();
		if ((elseItem != null) && !(elseItem instanceof Parameter) && !(elseItem instanceof Value)) {
			result = isDistillated(elseItem, state);
		}
		
		for (SqlObject item: caseItem) {
			if (item instanceof WhenThen) {
				WhenThen whenThenItem = (WhenThen) item;
				ColumnExpression thenItem = checkNotRecordId(whenThenItem.getThen(), state);
				if (!(thenItem instanceof Parameter) && !(thenItem instanceof Value)) {
					result = result && isDistillated(thenItem, state);
				}
			}
		}
		return result;
	}
	
	protected void distillateParamsAndValuesThenElse(Case caseItem, ColumnExprTechInfo info, DistillerState state)
			throws Exception{
		DistillationParamsIn paramsIn = new DistillationParamsIn(info.fieldTypeId);
		for (SqlObject item: caseItem) {
			if (item instanceof WhenThen) {
				WhenThen whenThenItem = (WhenThen) item;
				ColumnExpression thenItem = whenThenItem.getThen();
				if ((thenItem instanceof Value) || (thenItem instanceof Parameter)) {
					internalDistillateObject(new DistillerState(thenItem, paramsIn, state),
							VALUE_OR_PARAM, null);
				}
			}
		}
		if (caseItem.getElse() != null && (caseItem.getElse() instanceof Value || caseItem.getElse() instanceof Parameter))
            internalDistillateObject(new DistillerState(caseItem.getElse(), paramsIn, state),
                    VALUE_OR_PARAM, null);
    }


}
