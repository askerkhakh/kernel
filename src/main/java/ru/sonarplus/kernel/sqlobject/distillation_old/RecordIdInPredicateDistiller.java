package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public class RecordIdInPredicateDistiller extends CommonBaseDistiller {
	protected static final String REC_ID = "RecId";

	public RecordIdInPredicateDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source){
	  /* для условий:
		    - "... in(...,...,...)"
		    - (..., ..., ...) in(SELECT...) */
		
		return isRecordId(source) && ((source.getOwner() instanceof PredicateInTuple) ||
				((source.getOwner() instanceof TupleExpressions) && (source.getOwner().getOwner() instanceof PredicateIn)));
	}
	
	protected void distillateAtomRecId(ColumnExprTechInfo recId, String tableAlias, DistillerState state)
			throws SqlObjectException {
		QualifiedField qField = new QualifiedField(tableAlias, recId.nativeFieldName);
		qField.distTechInfo = recId;
		replace(state.sqlObject, qField);
	}
	
	protected Predicate getPredicate(DistillerState state) {
		if (state.sqlObject.getOwner() instanceof Predicate) {
			return (Predicate) state.sqlObject.getOwner();
		}
		else {
			return (Predicate) state.sqlObject.getOwner().getOwner();
		}
	}
	
	protected void checkCompositeRecordIdAllowed(DistillerState state, String tableAlias) {
		Predicate predicate = getPredicate(state);
		Preconditions.checkArgument(!(predicate instanceof PredicateInTuple),
				"В условии %s составной идентификатор записи %s недопустим.",
				state.sqlObject.getClass().getSimpleName(),
				QualifiedName.formQualifiedNameString(tableAlias, REC_ID));
		Preconditions.checkArgument(((PredicateInTuple)predicate).getTuple().itemsCount() == 1,
				"В условии %s кроме составного идентификатора записи %s в кортеже не должно быть других выражений",
				state.sqlObject.getClass().getSimpleName(),
				QualifiedName.formQualifiedNameString(tableAlias, REC_ID));
	}
	
	protected void distillateCompositeRecId(ColumnExprTechInfo[] recId, String tableAlias, DistillerState state)
			throws SqlObjectException {
		/* составной идентификатор записи разрешаем только в одном случае:
		    когда он в кортеже условия "() in(select)"
		    и при этом в кортеже нет других выражений */
		checkCompositeRecordIdAllowed(state, tableAlias);
		TupleExpressions tuple = (TupleExpressions) state.sqlObject.getOwner();
		for (ColumnExprTechInfo fieldSpec: recId) {
			QualifiedField qField = new QualifiedField(tableAlias, fieldSpec.nativeFieldName);
			qField.distTechInfo = fieldSpec;
			tuple.insertItem(qField);
		}
		state.sqlObject = null;
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws CloneNotSupportedException, SqlObjectException {
		String tableAlias = fieldAliasPart((QualifiedField)state.sqlObject);
		ColumnExprTechInfo[] recordId = state.namesResolver.recordIdFields(SqlObjectUtils.getParentQuery(state.sqlObject), tableAlias);
		if (recordId != null) {
			if (recordId.length == 1) {
				distillateAtomRecId(recordId[0], tableAlias, state);
			}
			else {
				distillateCompositeRecId(recordId, tableAlias, state);
			}
		}
		else {
		    // в этом контексте не удалось получить поля идентификатора записи, попробуем уровнем выше
			addNotDistillatedObject(state.sqlObject, state);
		}
		return null;
	}

}
