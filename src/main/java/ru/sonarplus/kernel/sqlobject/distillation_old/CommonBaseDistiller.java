package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.Conditions;
import ru.sonarplus.kernel.sqlobject.objects.Predicate;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;

import java.util.Set;

public abstract class CommonBaseDistiller extends CommonDistiller {

	protected static Set<Class<? extends SqlObject>> PREDICATE = getAllowedClasses(Predicate.class);

	public CommonBaseDistiller() {

	}
	
	protected SqlObject internalDistillateObject(DistillerState state, 
			Set<Class<? extends SqlObject>> allowedClasses,
			Set<Class<? extends SqlObject>> ignorableClasses)
			throws Exception{
		return Utils.distillateSqlObject(state.sqlObject, state.dbc, state.namesResolver, state.paramsIn,
				allowedClasses, ignorableClasses, state.objectsNotDistilated, state.paramsForWrapWithExpr);
	}

	protected void internalDistillatePredicate(Predicate predicate,
			DistillerState state)
			throws Exception{
		if (predicate instanceof Conditions) {
			for (SqlObject child: predicate)
				internalDistillatePredicate((Predicate)child, state);
		}
		else
			internalDistillateObject(new DistillerState(predicate, state), PREDICATE, null);
	}
}
