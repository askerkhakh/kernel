package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.RecIdValueWrapper;
import ru.sonarplus.kernel.sqlobject.objects.Predicate;
import ru.sonarplus.kernel.sqlobject.objects.PredicateIn;
import ru.sonarplus.kernel.sqlobject.objects.PredicateInTuple;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.objects.TupleExpressions;
import ru.sonarplus.kernel.sqlobject.objects.Value;
import ru.sonarplus.kernel.sqlobject.objects.ValueConst;
import ru.sonarplus.kernel.sqlobject.objects.ValueRecId;

public class ValueRecIdInPredicateDistiller extends CommonBaseDistiller {

	public ValueRecIdInPredicateDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
	  /* для условий:
		    - "... in(...,...,...)"
		    - (..., ..., ...) in(SELECT...) */
		return (source instanceof ValueRecId) && ((source.getOwner() instanceof PredicateInTuple) ||
				((source.getOwner() instanceof TupleExpressions) && (source.getOwner().getOwner() instanceof PredicateIn)));
	}
	
	protected FieldTypeId getValueType(Object value) {
		if (value == null) {
			return FieldTypeId.tid_LARGEINT;
		}
		else if (value instanceof Long) {
			return FieldTypeId.tid_LARGEINT;
		}
		else if (value instanceof Integer) {
			return FieldTypeId.tid_INTEGER;
		}
		else if (value instanceof String) {
			return FieldTypeId.tid_STRING;
		}
		else if (value instanceof byte[]) {
			return FieldTypeId.tid_CODE;
		}
		else {
			return FieldTypeId.tid_UNKNOWN;
		}
	}
	
	protected void distillateAtomValueRecId(RecIdValueWrapper valueRecId, DistillerState state)
			throws SqlObjectException {
		Object v = valueRecId.getValue(0);
		Value value = new ValueConst(v, getValueType(v));
		replace(state.sqlObject, value);
	}
	
	protected Predicate getPredicate(DistillerState state) {
		if (state.sqlObject.getOwner() instanceof Predicate) {
			return (Predicate) state.sqlObject.getOwner();
		}
		else {
			return (Predicate) state.sqlObject.getOwner().getOwner();
		}
	}
	
	protected void checkCompositeRecordIdAllowed(DistillerState state) {
		Predicate predicate = getPredicate(state);
		Preconditions.checkArgument(!(predicate instanceof PredicateInTuple),
				"В условии %s значение составного идентификатора записи недопустимо.",
				state.sqlObject.getClass().getSimpleName());
		Preconditions.checkArgument(((PredicateInTuple)predicate).getTuple().itemsCount() == 1,
				"В условии %s кроме значения составного идентификатора записи в кортеже не должно быть других выражений",
				state.sqlObject.getClass().getSimpleName());
	}
	
	
	protected void distillateCompositeValueRecId(RecIdValueWrapper valueRecId, DistillerState state)
			throws SqlObjectException {
		checkCompositeRecordIdAllowed(state);
		TupleExpressions tuple = (TupleExpressions) state.sqlObject.getOwner();
		for (int i = 0; i < valueRecId.count(); i++) {
			Object v = valueRecId.getValue(i);
			Value value = new ValueConst(v, getValueType(v));
			tuple.insertItem(value);
		}
		state.sqlObject = null;
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws CloneNotSupportedException, SqlObjectException {
		RecIdValueWrapper valueRecId = ((ValueRecId) state.sqlObject).getRecId();
		if (valueRecId.count() == 1) {
			distillateAtomValueRecId(valueRecId, state);
		}
		else {
			distillateCompositeValueRecId(valueRecId, state);
		}
		return null;
	}

}
