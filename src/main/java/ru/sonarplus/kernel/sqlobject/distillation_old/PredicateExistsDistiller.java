package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.PredicateExists;
import ru.sonarplus.kernel.sqlobject.objects.Select;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

public class PredicateExistsDistiller extends CommonBaseDistiller {

	public PredicateExistsDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof PredicateExists;
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		PredicateExists predicate = (PredicateExists) state.sqlObject;
		Select select = predicate.getSelect();
		Utils.distillateQueryEx(select, state.dbc, false, state.objectsNotDistilated, state.paramsForWrapWithExpr);
		return predicate;
	}

}
