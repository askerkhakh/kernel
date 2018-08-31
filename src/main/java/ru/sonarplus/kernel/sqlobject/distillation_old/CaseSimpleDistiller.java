package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.objects.CaseSimple.WhenThen;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public class CaseSimpleDistiller extends CaseDistiller {

	public CaseSimpleDistiller() {

	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof CaseSimple;
	}
	
	protected boolean tryDistilateCaseWhenClauseWithoutValues(CaseSimple caseSimple, DistillerState state)
			throws Exception{
		boolean result = true;
		ColumnExpression caseItem = caseSimple.getCase();
		if (!(caseItem instanceof Parameter) && !(caseItem instanceof Value)) {
			result = isDistillated(caseItem, state);
		}
		
		for (SqlObject item: caseSimple)
			if (item instanceof WhenThen) {
                WhenThen whenThen = (WhenThen) item;
                ColumnExpression when = checkNotRecordId(whenThen.getWhen(), state);
                if (!(when instanceof Parameter) && !(when instanceof Value)) {
                    result = result && isDistillated(when, state);
			    }
			}
		return result;
	}
	
	protected ColumnExprTechInfo getCommonTechInfoForCaseWhenClause(CaseSimple caseSimple, DistillerState state) {
		ColumnExprTechInfo fieldTechInfoPrev = SqlObjectUtils.getTechInfo(caseSimple.getCase());
		Boolean isCaseSensitive = fieldTechInfoPrev.techInfoPrepared ? fieldTechInfoPrev.caseSensitive : null;
		FieldTypeId fieldType = fieldTechInfoPrev.fieldTypeId;
		
		for (SqlObject item: caseSimple)
		    if (item instanceof WhenThen) {
                WhenThen whenThen = (WhenThen) item;
                ColumnExprTechInfo fieldTechInfo = SqlObjectUtils.getTechInfo(whenThen.getWhen());
                if (fieldTechInfo.techInfoPrepared) {
                    isCaseSensitive = getIsCS(isCaseSensitive, fieldTechInfo);
                    checkTypes(fieldTechInfoPrev.fieldTypeId, fieldTechInfo.fieldTypeId, "CASE/WHEN", state);
                    fieldTechInfoPrev = fieldTechInfo;
                    fieldType = getType(fieldTechInfoPrev, fieldType);
			    }
		    }

		ColumnExprTechInfo result = new ColumnExprTechInfo();
		result.fieldTypeId = fieldType;
		result.caseSensitive = (isCaseSensitive == null) || isCaseSensitive;
		return result;
	}
	
	protected  void applyCSToCaseWhen(CaseSimple caseItem, DistillerState state, boolean isCaseSensitive)
			throws SqlObjectException {
		if (isCaseSensitive) {
			return;
		}
		wrapWithUpper(caseItem.getCase(), false, state);
		for (SqlObject item: caseItem) {
			WhenThen whenThen = (WhenThen) item;
			wrapWithUpper(whenThen.getWhen(), false, state);
		}
	}
	
	protected void distillateParamsAndValuesCaseWhen(CaseSimple caseItem, ColumnExprTechInfo info, DistillerState state)
			throws Exception{
		DistillationParamsIn paramsIn = new DistillationParamsIn(info.fieldTypeId);
		ColumnExpression caseElement = caseItem.getCase();
		if ((caseElement instanceof Value) || (caseElement instanceof Parameter)) {
			internalDistillateObject( new DistillerState(caseElement, paramsIn, state), VALUE_OR_PARAM, null);
		}
		for (SqlObject item: caseItem)
		    if (item instanceof WhenThen){
			    WhenThen whenThen = (WhenThen) item;
			    ColumnExpression when = whenThen.getWhen();
			    if ((when instanceof Value) || (when instanceof Parameter)) {
				    internalDistillateObject( new DistillerState(when, paramsIn, state), VALUE_OR_PARAM, null);
			    }
		    }
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		CaseSimple caseSimple = (CaseSimple) state.sqlObject;
		if (tryDistilateCaseWhenClauseWithoutValues(caseSimple, state) && 
				tryDistilateThenElseClauseWithoutValues(caseSimple, state)) {
			// все выражения (кроме значений) были наконец отдистиллированы - есть описатели
		    // проверим совместимось типов значений, возвращаемых выражениями в разделах CASE/WHEN и THEN/ELSE
			ColumnExprTechInfo caseWhenTechInfo = getCommonTechInfoForCaseWhenClause(caseSimple, state);

			// применим регистрозависимость к выражениям в CASE/WHEN
			applyCSToCaseWhen(caseSimple, state, caseWhenTechInfo.caseSensitive);
 	        // выполним дистиляцию значений/параметров с учётом типов значений
			distillateParamsAndValuesCaseWhen(caseSimple, caseWhenTechInfo, state);
			distillateParamsAndValuesThenElse(caseSimple, getValueTypeForThenElseClause(caseSimple, state), state);
			caseSimple.distTechInfo = getValueTypeForThenElseClause(caseSimple, state);
            caseSimple.distTechInfo.techInfoPrepared = true;
		}
		else {
			addNotDistillatedObject(caseSimple, state);
		}
		return caseSimple;
	}

}
