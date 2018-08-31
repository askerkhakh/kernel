package ru.sonarplus.kernel.sqlobject.distillation_old;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.ArrayList;
import java.util.List;

public class RecordIdColumnDistiller extends CommonBaseDistiller {
	public SqlQuery parent; 

	public RecordIdColumnDistiller() {
	}

	@Override
	public boolean isMy(SqlObject source) {
		return SqlObjectUtils.isRecordId(source) && (source.getOwner() instanceof SelectedColumn);
	}
	
	protected boolean isNoSameColumn(QualifiedField recordId, String fieldAlias, ColumnExprTechInfo
			field) {
		Select select = (Select) parent;
		SelectedColumn recordIdColumn = (SelectedColumn) recordId.getOwner();
		SelectedColumnsContainer columns = select.getColumns();
		if (columns != null) {
			for (SqlObject columnItem: columns) {
				if ((columnItem != recordIdColumn) && (((SelectedColumn)columnItem).alias.equals(fieldAlias))) {
					return false;
				}
			}
		}
		return true;
	}
	
	protected void distillateForMultipleFields(QualifiedField recordId, ColumnExprTechInfo[] fields)
			throws SqlObjectException {
		List<SqlObject> tmpColumns = new ArrayList<SqlObject>(); 
		SelectedColumn recordIdColumn = (SelectedColumn) recordId.getOwner();
		SelectedColumnsContainer parentColumns = (SelectedColumnsContainer) recordIdColumn.getOwner();
		for (ColumnExprTechInfo recordIdField: fields) {
			if (isNoSameColumn(recordId, recordIdField.nativeFieldName, recordIdField)) {
				QualifiedField qField = new QualifiedField(recordId.alias, recordIdField.nativeFieldName);
				qField.distTechInfo = recordIdField;
				SelectedColumn column = new SelectedColumn();
				column.setExpression(qField);
				column.alias = recordIdField.nativeFieldName;
				tmpColumns.add(column);
			}
		}
		parentColumns.replaceWithSet(recordIdColumn, tmpColumns.toArray(new SqlObject[0]));
	}
	
	protected void distillateForOneField(QualifiedField recordId, ColumnExprTechInfo field)
			throws SqlObjectException {
		SelectedColumn oldColumn = (SelectedColumn) recordId.getOwner();
		if (isNoSameColumn(recordId, field.nativeFieldName, field)) {
			QualifiedField qField = new QualifiedField(recordId.alias, field.nativeFieldName);
			qField.distTechInfo = field;
			SelectedColumn newColumn = new SelectedColumn();
			newColumn.setExpression(qField);
			newColumn.alias = field.nativeFieldName;
			replace(oldColumn, newColumn);
		}
		else {
			((SelectedColumnsContainer) oldColumn.getOwner()).replaceWithSet(oldColumn, (SqlObject[]) null);
		}
	}

	@Override
	protected SqlObject internalDistillate(DistillerState state)
			throws CloneNotSupportedException, SqlObjectException {
		QualifiedField recId = (QualifiedField) state.sqlObject;
		
	    // идентификатор записи таким образом дистиллируется только в разделе колонок,
		//  для дистилляции в составе условия сравнения по идентификатору - другой метод
		Preconditions.checkArgument((recId.getOwner() != null) && (recId.getOwner() instanceof SelectedColumn));
		parent = SqlObjectUtils.getParentQuery(recId);
		recId.alias = fieldAliasPart(recId);
		ColumnExprTechInfo[] recordId = state.namesResolver.recordIdFields(parent, recId.alias);
		if (recordId.length != 1) {
			distillateForMultipleFields(recId, recordId);
		}
		else {
			distillateForOneField(recId, recordId[0]);
		}
		return null;
		
	}

}
