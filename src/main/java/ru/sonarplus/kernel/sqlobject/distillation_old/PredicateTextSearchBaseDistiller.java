package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExpression;
import ru.sonarplus.kernel.sqlobject.objects.PredicateTextSearch;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import java.util.Set;

public abstract class PredicateTextSearchBaseDistiller extends CommonBaseDistiller {
	protected static final Set<Class<? extends SqlObject>> COLUMN_EXPRESSION = getAllowedClasses(ColumnExpression.class);

	protected void distillateItem(ColumnExpression item, DistillerState state)
			throws Exception{
		if (!Utils.isTechInfoFilled(item)) {
			internalDistillateObject(new DistillerState(item, new DistillationParamsIn(FieldTypeId.tid_UNKNOWN), state),
					COLUMN_EXPRESSION, null);
		}
		
	}
	
	protected boolean tryDistilateExprAndTemplate(PredicateTextSearch predicate, DistillerState state)
			throws Exception{
		ColumnExpression expr = predicate.getExpr();
		distillateItem(expr, state);
		ColumnExpression template = predicate.getTemplate();
		distillateItem(template, state);
		return Utils.isTechInfoFilled(expr) && Utils.isTechInfoFilled(template);
	}


}
