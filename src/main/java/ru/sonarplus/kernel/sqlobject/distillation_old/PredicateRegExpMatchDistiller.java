package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.PredicateRegExpMatch;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

public class PredicateRegExpMatchDistiller extends PredicateTextSearchBaseDistiller {

	public PredicateRegExpMatchDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof PredicateRegExpMatch;
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		PredicateRegExpMatch predicate = (PredicateRegExpMatch) state.sqlObject;
		if (!tryDistilateExprAndTemplate(predicate, state)) {
			addNotDistillatedObject(predicate, state);
		}
		return predicate;
	}

}
