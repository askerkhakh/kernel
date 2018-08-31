package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.CaseSearch;
import ru.sonarplus.kernel.sqlobject.objects.CaseSearch.WhenThen;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

public class CaseSearchDistiller extends CaseDistiller {

	public CaseSearchDistiller() {

	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof CaseSearch;
	}
	
	protected void distillateWhen(CaseSearch caseItem, DistillerState state)
			throws Exception{
		for (SqlObject item: caseItem) {
			if (item instanceof WhenThen) {
				WhenThen whenThen = (WhenThen) item;
				internalDistillatePredicate(whenThen.getWhen(), state);
			}
		}
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		CaseSearch caseSearch = (CaseSearch) state.sqlObject;
		distillateWhen(caseSearch, state);
		if (tryDistilateThenElseClauseWithoutValues(caseSearch, state)) {
		      distillateParamsAndValuesThenElse(caseSearch, getValueTypeForThenElseClause(caseSearch, state), state);
		      caseSearch.distTechInfo = getValueTypeForThenElseClause(caseSearch, state);
			  caseSearch.distTechInfo.techInfoPrepared = true;
		}
		else {
			addNotDistillatedObject(caseSearch, state);
		}
		return caseSearch;
	}

}
