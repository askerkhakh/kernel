package ru.sonarplus.kernel.sqlobject.objects;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.CodeTableInfo;
import ru.sonarplus.kernel.dbschema.CodeTypeSpec;
import ru.sonarplus.kernel.dbschema.DataTypeSpec;
import ru.sonarplus.kernel.dbschema.DbSchemaUtils;
import ru.sonarplus.kernel.dbschema.FieldSpec;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.dbschema.IndexSpec;
import ru.sonarplus.kernel.dbschema.IndexType;
import ru.sonarplus.kernel.dbschema.StringTypeSpec;

public class ColumnExprTechInfo implements Cloneable {

	public FieldSpec fieldSpec;
	public String dbdFieldName;
	public String nativeFieldName;
	public FieldTypeId fieldTypeId;
	public boolean caseSensitive;
	public int bytesForCode;
	public String csbNativeFieldName;
	public boolean originFieldIsUnique;
	public String originTableName;
	public boolean fullTextIndex;
	public String functionNameForIndex;
	public boolean techInfoPrepared;
	public String tableExprName;

	public ColumnExprTechInfo() {
		init();
	}

	public ColumnExprTechInfo getClone() {
		try {
			return clone();
		}
		catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}

	public void init() {
		dbdFieldName = "";
		nativeFieldName = "";
		fieldTypeId = FieldTypeId.tid_UNKNOWN;
		caseSensitive = true;
		bytesForCode = 0;
		csbNativeFieldName = "";
		originFieldIsUnique = false;
		originTableName = "";
		techInfoPrepared = false;
		resetIndexInfo();
		
	}
	
	public void resetIndexInfo() {
		fullTextIndex = false;
		functionNameForIndex = "";
	}
	
	public ColumnExprTechInfo clone() throws CloneNotSupportedException {
		return (ColumnExprTechInfo) super.clone();
	}	
	
	public static ColumnExprTechInfo createTechInfoByFieldSpec(FieldSpec fieldSpec) {
		Preconditions.checkNotNull(fieldSpec);
		ColumnExprTechInfo result = new ColumnExprTechInfo();
		result.fieldSpec = fieldSpec;
		result.dbdFieldName = fieldSpec.getFieldName();
		result.nativeFieldName = fieldSpec.getLatinName();
		DataTypeSpec dataTypeSpec = fieldSpec.getDataTypeSpec();
		result.fieldTypeId = dataTypeSpec.getFieldTypeId();
		if (dataTypeSpec instanceof StringTypeSpec) {
			result.caseSensitive = ((StringTypeSpec) dataTypeSpec).getIsCaseSensitive();
		}
		result.originFieldIsUnique = DbSchemaUtils.isUniquField(fieldSpec);
		if (result.fieldTypeId == FieldTypeId.tid_CODE) {
			Preconditions.checkArgument(dataTypeSpec instanceof CodeTypeSpec);
			result.bytesForCode = ((CodeTypeSpec) dataTypeSpec).bytesForCode;
			FieldSpec serviceFieldSpec = fieldSpec.tableSpec.findFieldSpecByName(CodeTableInfo.CODE_SERVICE_FIELD);
			if ((serviceFieldSpec != null) && result.originFieldIsUnique) {
				result.csbNativeFieldName = serviceFieldSpec.latinName;
			}
		}
		result.originTableName = fieldSpec.getTableSpec().getName();
		IndexSpec indexSpec = DbSchemaUtils.getIndexByField(fieldSpec, IndexType.FULLTEXT);
		if (indexSpec != null) {
			String functionName = DbSchemaUtils.getFunctionNameForIndex(indexSpec);
			if (functionName != null) {
				result.fullTextIndex = true;
				result.functionNameForIndex = functionName;
				
			}
		}
		result.techInfoPrepared = true;
		return result;
	}
}
