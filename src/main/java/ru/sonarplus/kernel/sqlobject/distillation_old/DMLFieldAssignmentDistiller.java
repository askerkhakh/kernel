package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;

import java.util.Set;

public class DMLFieldAssignmentDistiller extends CommonBaseDistiller {
	protected static final Set<Class<? extends SqlObject>> QUALIFIED_FIELD = getAllowedClasses(QualifiedField.class);
	protected static final Set<Class<? extends SqlObject>> COLUMN_EXPRESSION = getAllowedClasses(ColumnExpression.class);

	public DMLFieldAssignmentDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return source instanceof DMLFieldAssignment;
	}
	
	protected DataChangeSqlQuery getDml(DMLFieldAssignment assignment) {
		Preconditions.checkArgument((assignment.getOwner() != null) && (assignment.getOwner() instanceof DMLFieldsAssignments));
		DMLFieldsAssignments assignmentsItem = (DMLFieldsAssignments) assignment.getOwner();
		Preconditions.checkNotNull(assignmentsItem.getOwner());
		return (DataChangeSqlQuery) assignmentsItem.getOwner();
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws Exception{
		DMLFieldAssignment assignment = (DMLFieldAssignment) state.sqlObject;
		QualifiedField field = assignment.getField();
		Preconditions.checkNotNull(field);
		DataChangeSqlQuery dmlQuery = getDml(assignment);
		if ((dmlQuery instanceof SqlQueryUpdate) || 
				((dmlQuery instanceof SqlQueryInsert) && (((SqlQueryInsert) dmlQuery).findSelect() == null))) {
			Preconditions.checkNotNull(assignment.getExpr());
		}
		internalDistillateObject( new DistillerState(field,
				state.dbc,
				state.namesResolver,
				new DistillationParamsIn(FieldTypeId.tid_UNKNOWN),
				state.objectsNotDistilated,
				state.paramsForWrapWithExpr),
				QUALIFIED_FIELD, null);
		
		/* Всем объектам QualifiedRField после дистиляции устанавливается алиасная часть - имя/алиас таблицы.
          Но имена полей, которым присваиваются значения в запросах INSERT/UPDATE,
          должны быть неквалифицированными, т.е. не должно указываться имя таблицы запроса.
          Oracle при выполнении запроса понимает квалифицированные имена, а, например, SQLite выдаёт синтаксическую ошибку.   */
		assignment.getField().alias = "";
		ColumnExpression expr = assignment.getExpr();
		if (expr != null) {
			Preconditions.checkState(Utils.isTechInfoFilled(assignment.getField()));
			ColumnExpression newExpr = (ColumnExpression) internalDistillateObject(new DistillerState (expr, state.dbc, state.namesResolver,
					new DistillationParamsIn(Utils.getFieldTypeId(field)),
					state.objectsNotDistilated,
					state.paramsForWrapWithExpr), COLUMN_EXPRESSION, null);
			Preconditions.checkNotNull(newExpr);
			wrapWithUpper(newExpr, Utils.isCaseSensitive(field), state);
		}
		return null;
		
	}

	
}
