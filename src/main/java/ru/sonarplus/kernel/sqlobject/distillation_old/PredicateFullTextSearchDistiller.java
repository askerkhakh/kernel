package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.db_support.FullTextEngine;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public class PredicateFullTextSearchDistiller extends PredicateTextSearchBaseDistiller {

	public PredicateFullTextSearchDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof PredicateFullTextSearch;
	}
	
	protected Predicate distillateFTS(PredicateFullTextSearch fts, DistillerState state)
			throws Exception{
		Predicate result = state.dbc.dbSupport.createFTSExpression(FullTextEngine.ORACLE_TEXT, fts);
		Preconditions.checkNotNull(result);
		Preconditions.checkState((result instanceof PredicateComparison) ||
				(result instanceof PredicateLike) || (result instanceof Conditions),
				"В результате дистиляции предиката полнотекстового поиска получен результат неизвестного типа.");
        // все содержащиеся в условии строковые выражения, построенные с помощью функций EXPR_...
	    // сконвертируем в деревья объектов
		SqlObjectUtils.buildTreesForExpressions(result);
		replace(fts, result);
		internalDistillatePredicate(result, state);
		return result;
	}

	@Override
    protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
        PredicateFullTextSearch predicate = (PredicateFullTextSearch)state.sqlObject;
        ColumnExpression left = predicate.getLeft();
        if (left instanceof QualifiedRField) {
            QualifiedRField qfield = (QualifiedRField) left;
            qfield.alias = fieldAliasPart(qfield);
        }
        return distillateFTS(predicate, state);
    }

}
