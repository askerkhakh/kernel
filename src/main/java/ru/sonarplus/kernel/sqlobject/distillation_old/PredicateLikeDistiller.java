package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.PredicateLike;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

public class PredicateLikeDistiller extends PredicateTextSearchBaseDistiller {

	public PredicateLikeDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof PredicateLike;
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		PredicateLike predicate = (PredicateLike) state.sqlObject;
	if (tryDistilateExprAndTemplate(predicate, state)) {
		ColumnExpression expr = predicate.getExpr();
		ColumnExpression template = predicate.getTemplate();
		boolean isCaseSensitive = Utils.isCaseSensitive(expr) && Utils.isCaseSensitive(template);
		wrapWithUpper(expr, isCaseSensitive, state);
		wrapWithUpper(template, isCaseSensitive, state);
	}
	return predicate;
	}

}
