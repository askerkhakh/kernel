package ru.sonarplus.kernel.dbschema;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;


public class DbSchemaUtils {

	public DbSchemaUtils() {
		// TODO Auto-generated constructor stub
	}
	
	public static ConstraintWithDetailsSpec getConstraintByField(
			  FieldSpec fieldSpec,
			  ConstraintType constraintType) {
		Preconditions.checkNotNull(fieldSpec);
		for (ConstraintSpec constraint: fieldSpec.tableSpec.constraintItems) {
			if ((constraint.constraintType == constraintType) &&
			(constraint instanceof ConstraintWithDetailsSpec)) {
				ConstraintWithDetailsSpec details = (ConstraintWithDetailsSpec) constraint;
				if (details.items.size() == 1) {
					if (details.items.get(0).getFieldName().equals(fieldSpec.getFieldName())) {
						return details; 
					}
				}
			}
		}
		return null;
		
	}
	
	public static IndexSpec getIndexByField(FieldSpec fieldSpec, IndexType indexType) {
		Preconditions.checkNotNull(fieldSpec);
		for (IndexSpec index: fieldSpec.tableSpec.indexItems) {
			if ((index.getIndexType() == indexType) && (index.items.size() == 1)) {
				FieldSpec indexField = index.items.get(0).getFieldSpec(); 
				if ((indexField != null) && indexField.getFieldName().equals(fieldSpec.getFieldName())) {
					return index;
				}
			}
		}
		return null;
		
	}
	
	public static boolean isUniquField(FieldSpec fieldSpec) {
		return (getConstraintByField(fieldSpec, ConstraintType.PRIMARY) != null) ||
				(getConstraintByField(fieldSpec, ConstraintType.UNICAL) != null) ||
				(getIndexByField(fieldSpec, IndexType.UNIQUE) != null);
	}
	
	public static String getFunctionNameForIndex(IndexSpec indexSpec) {
		return null;
		//TODO определять имя функции для полнотектового индекса по атрибутам после реализации загрузки атрибутов
	}

	@Nullable
	public static FieldSpec getPrimaryKeyFieldSpec(@Nullable TableSpec tableSpec) {
		if (tableSpec == null) {
			return null;
		}
		PrimaryKeyConstraintSpec primaryKey = tableSpec.getPrimaryKey();
		if (primaryKey == null) {
			return null;
		}
		if (primaryKey.getItemsCount() != 1) {
			return null;
		}
		return primaryKey.getItem(0);
	}

}
