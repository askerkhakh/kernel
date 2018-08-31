package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.objects.ColumnExprTechInfo;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedField;
import ru.sonarplus.kernel.sqlobject.objects.QualifiedRField;
import ru.sonarplus.kernel.sqlobject.objects.SqlObject;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

public class QualifiedFieldDistiller extends CommonBaseDistiller {

	public QualifiedFieldDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return Utils.isQField(source);
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws CloneNotSupportedException, SqlObjectException {
		QualifiedField qField = (QualifiedField) state.sqlObject; 
		if (!(qField instanceof QualifiedRField)) {
		      // с полем, заданным латиницей, ничего не делаем
			if (! Utils.isTechInfoFilled(qField)) {
				SqlObjectUtils.buildTechInfoForExpr(qField, FieldTypeId.tid_UNKNOWN);
			}
			return qField;
		}
		QualifiedName qname = new QualifiedName(fieldAliasPart(qField), qField.fieldName);
		ColumnExprTechInfo fieldSpec = state.namesResolver.resolveName(Utils.getParentQueryEx(qField), QualifiedName.formQualifiedNameString(qname.alias, qname.name)); 
		if (fieldSpec != null) {
			QualifiedField result = new QualifiedField(null, qname.alias, fieldSpec.nativeFieldName);
			result.distTechInfo = fieldSpec;
			replace(qField, result);
			return result;
		}
		else {
		    // такое может быть, если мы дистилируем подзапрос в предикате "Exists"( или "in(select)"), в котором используется поле из верхнего запроса
			addNotDistillatedObject(qField, state);
			return null;
		}
	}

}
