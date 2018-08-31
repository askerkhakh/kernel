package ru.sonarplus.kernel.dbschema_loader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import ru.sonarplus.kernel.dbschema.Alignment;
import ru.sonarplus.kernel.dbschema.CheckConstraintSpec;
import ru.sonarplus.kernel.dbschema.CodeTypeSpec;
import ru.sonarplus.kernel.dbschema.ConstraintSpec;
import ru.sonarplus.kernel.dbschema.ConstraintType;
import ru.sonarplus.kernel.dbschema.ConstraintWithDetailsSpec;
import ru.sonarplus.kernel.dbschema.DataTypeSpec;
import ru.sonarplus.kernel.dbschema.DbSchemaSpec;
import ru.sonarplus.kernel.dbschema.DbdItemWithAttributes;
import ru.sonarplus.kernel.dbschema.DomainSpec;
import ru.sonarplus.kernel.dbschema.FieldSpec;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.dbschema.FloatTypeSpec;
import ru.sonarplus.kernel.dbschema.ForeignKeyConstraintSpec;
import ru.sonarplus.kernel.dbschema.IndexItemSpec;
import ru.sonarplus.kernel.dbschema.IndexOrder;
import ru.sonarplus.kernel.dbschema.IndexSpec;
import ru.sonarplus.kernel.dbschema.IndexType;
import ru.sonarplus.kernel.dbschema.NumberTypeSpec;
import ru.sonarplus.kernel.dbschema.PrimaryKeyConstraintSpec;
import ru.sonarplus.kernel.dbschema.RealTypeSpec;
import ru.sonarplus.kernel.dbschema.SDataTypes;
import ru.sonarplus.kernel.dbschema.StringTypeSpec;
import ru.sonarplus.kernel.dbschema.TableSpec;
import ru.sonarplus.kernel.dbschema.UniqueKeyConstraintSpec;


public class Loader {
	private static final String SQLITE_JDBC_DRIVER = "org.sqlite.JDBC";
	private static final String JDBC_SQLITE_PREFIX = "jdbc:sqlite:";
	private static final String DBD_SPECIFIC = "dbd.specific";
	private static final String DBD_VERSION = "dbd.version";
	private static final int DBD_CURRENT_VERSION = 4;
	private static final int MAX_DISPLAY_WIDTH = 80;
	private static final String  DBD_ATTR_EXTERNAL_TABLE = "{D38E1B96-B2C1-4908-B073-164863D46BEC}";
	private static final String  DBD_ATTR_USED_ADDITIONS_ATTR_GUID = "{12B871D8-15CA-4B9C-8FE9-5A8C94DAAF58}";


	public Loader() {
		// TODO Auto-generated constructor stub
	}
	
	protected static String getDbdSettingByKey(Connection connection, String key) throws SQLException {
		PreparedStatement stmt = connection.prepareStatement("select value from dbd$settings where key = ?");
		try {
			stmt.setString(1, key);
			ResultSet rs = stmt.executeQuery();
			try {
				if (rs.next()) {
					return rs.getString(1);
				}
				else {
					return "";
				}
			}
			finally {
				rs.close();
			}
		}
		finally {
			stmt.close();
		}
		
	}
	
	protected static void chechDbdVersionIsActual(String version){
		String items[] = version.split(Pattern.quote("."));
		Preconditions.checkArgument(Integer.parseInt(items[0]) == DBD_CURRENT_VERSION,
				"Данная версия приложения не предназначена для работы с описателем версии %s. "+
				"Ожидалась версия описателя %d.0 или совместимая с ней. Обратитесь к администратору.",
				version, DBD_CURRENT_VERSION);
	}
	
	protected static Map<String,Integer> buildColumnsMap(ResultSetMetaData meta) throws SQLException {
		Map<String,Integer> result = new HashMap<String,Integer>();
		int columnCount = meta.getColumnCount();
		for (int i = 1; i <= columnCount; i++) {
			result.put(meta.getColumnName(i), i);
		}
		return result;
	}
	
	protected static int getColumnNo(Map<String,Integer> columnsMap, String name) {
		Integer result = columnsMap.get(name);
		Preconditions.checkArgument(result != null, "Не найдено поле "+name);
		return result;
	}
	
	public static class DomainsLoadContext {
		int fieldID;
		int fieldDomainName;
		int fieldDomainDescription;
		int fieldTypeId;
		int fieldAlign;
		int fieldPrecision;
		int fieldScale;
		int fieldShowNull;
		int fieldShowLeadingNull;
		int fieldHasThousandSeparator;
		int fieldSummable;
		int fieldCaseSensitive;
		int fieldSize;
		int fieldCharLength;
		int fieldDisplayWidth;
		
		public DomainsLoadContext(Map<String,Integer> columnsMap) {
			fieldID = getColumnNo(columnsMap, "id");
			fieldDomainName = getColumnNo(columnsMap, "name");
			fieldDomainDescription = getColumnNo(columnsMap, "description");
			fieldTypeId = getColumnNo(columnsMap, "type_id");
			fieldAlign = getColumnNo(columnsMap, "align");
			fieldPrecision = getColumnNo(columnsMap, "precision");
			fieldScale = getColumnNo(columnsMap, "scale");
			fieldShowNull = getColumnNo(columnsMap, "show_null");
			fieldShowLeadingNull = getColumnNo(columnsMap, "show_lead_nulls");
			fieldHasThousandSeparator = getColumnNo(columnsMap, "thousands_separator");
			fieldSummable = getColumnNo(columnsMap, "summable");
			fieldCaseSensitive = getColumnNo(columnsMap, "case_sensitive");
			fieldSize = getColumnNo(columnsMap, "length");
			fieldCharLength = getColumnNo(columnsMap, "char_length");
			fieldDisplayWidth = getColumnNo(columnsMap, "width");
		}
	}
	
	
	
	
	protected static int defaultTypeIdPrecision(FieldTypeId typeId) {
		switch (typeId) {
			case tid_SMALLINT:
				return 5;
			case tid_INTEGER:
				return 10;
			case tid_WORD:
				return 5;
			case tid_BOOLEAN:
				return 1;
			case tid_FLOAT:
				return 15;
			case tid_LARGEINT:
				return 20;
			case tid_BYTE:
				return 3;
			default:
				return 0;
		}
	}
	
	protected static int defaultTypeIdStrLen(FieldTypeId typeId) {
		int result = 0;
		switch (typeId) {
			case tid_STRING:
				result = 255;
				break;
			case tid_SMALLINT:
				result = 4;
				break;
			case tid_INTEGER:
				result = 11;
				break;
			case tid_WORD:
				result = 5;
				break;
			case tid_BOOLEAN:
				result = 3;
				break;
			case tid_FLOAT:
				result = 17;
				break;
			case tid_DATE:
				result = 10;
				break;
			case tid_TIME:
				result = 8;
				break;
			case tid_DATETIME:
				result = 17;
				break;
			case tid_MEMO:
				result = 255;
				break;
			case tid_LARGEINT:
				result = 20;
				break;
			case tid_BYTE:
				result = 3;
				break;
			default:
				break;
		}
		return result;
	}
	
	protected static DataTypeSpec createEmptyTypeSpec(String fieldTypeId, String name,
			int displayWidth, Alignment alignment) {
		DataTypeSpec result = null;
		FieldTypeId typeId = FieldTypeId.fromString(fieldTypeId);
		Preconditions.checkNotNull(typeId,"Неизвестный тип данных "+fieldTypeId);
		if (typeId == FieldTypeId.tid_CODE) {
			result = new CodeTypeSpec();
		}
		else if ((typeId == FieldTypeId.tid_SMALLINT) || (typeId == FieldTypeId.tid_INTEGER) ||
				(typeId == FieldTypeId.tid_WORD) || (typeId == FieldTypeId.tid_LARGEINT) || 
				(typeId == FieldTypeId.tid_BYTE)) {
			result = new NumberTypeSpec();
		}
		else if (typeId == FieldTypeId.tid_FLOAT) {
			result = new FloatTypeSpec();
		}
		else if (typeId == FieldTypeId.tid_STRING) {
			result = new StringTypeSpec();
		}
		else {
			result = new DataTypeSpec();
		}
		result.fieldTypeId = typeId;
		if (displayWidth > 0) {
			result.displayWidth = displayWidth; 
		}
		else {
			result.displayWidth = defaultTypeIdStrLen(typeId);
		}
		result.alignment = alignment; 
		return result;
	}
	
	protected static Alignment stringToAlignment(String text) {
		if (text == null) {
			return Alignment.LEFT;
		}
		switch (text) {
			case "R":
				return Alignment.RIGHT;
			case "C":
				return Alignment.CENTER;
			default:
				return Alignment.LEFT;
		}
	}
	
	
	protected static DataTypeSpec createDataTypeSpec(ResultSet rs, DomainsLoadContext context) throws SQLException {
		String fieldTypeStr = rs.getString(context.fieldTypeId);
		int displayWidth = rs.getInt(context.fieldDisplayWidth);
		String alignStr = rs.getString(context.fieldAlign);
		Alignment align = stringToAlignment(alignStr);
		
		DataTypeSpec result = createEmptyTypeSpec(fieldTypeStr, "", displayWidth, align);
		if (result instanceof CodeTypeSpec) {
			CodeTypeSpec codeSpec = (CodeTypeSpec) result; 
			codeSpec.posPerPart = rs.getInt(context.fieldPrecision); 
			codeSpec.bytesForCode = rs.getInt(context.fieldSize); 
		}
		else if (result instanceof NumberTypeSpec) {
			NumberTypeSpec numberSpec = (NumberTypeSpec) result;
			numberSpec.showNull = rs.getBoolean(context.fieldShowNull);
			numberSpec.showLeadingNull = rs.getBoolean(context.fieldShowLeadingNull);
			numberSpec.summable = rs.getBoolean(context.fieldSummable);
			numberSpec.hasThousandSeparator = rs.getBoolean(context.fieldHasThousandSeparator);
			numberSpec.precision = rs.getInt(context.fieldPrecision);
			if (numberSpec.precision == 0) {
				numberSpec.precision = defaultTypeIdPrecision(result.fieldTypeId);
			}
			if (result instanceof RealTypeSpec) {
				((RealTypeSpec) result).scale = rs.getInt(context.fieldScale);
			}
		}
		else if (result instanceof StringTypeSpec) {
			StringTypeSpec stringSpec = (StringTypeSpec) result;
			stringSpec.isCaseSensitive = rs.getBoolean(context.fieldCaseSensitive);
			stringSpec.charLength = rs.getInt(context.fieldCharLength);
			if (displayWidth == 0) {
				if (stringSpec.charLength <= MAX_DISPLAY_WIDTH) {
					stringSpec.displayWidth = stringSpec.charLength;
				}
				else {
					stringSpec.displayWidth = MAX_DISPLAY_WIDTH;
				}
			}
		}
		return result;
	}
	
	protected static DomainSpec createDomainSpec(ResultSet rs, DomainsLoadContext context) throws SQLException {
		DomainSpec domainSpec = new DomainSpec();
		domainSpec.name = rs.getString(context.fieldDomainName);
		domainSpec.description = rs.getString(context.fieldDomainDescription);
		domainSpec.dataTypeSpec = createDataTypeSpec(rs, context);
		return domainSpec;
		
	}
	
	protected static void loadDomainSpecs(Connection connection, DbSchemaSpec dbSchemaSpec, 
			Map<Integer, DomainSpec> domainSpecByIdMap) throws SQLException {
		PreparedStatement stmt = connection.prepareStatement("select * from dbd$view_domains");
		try {
			ResultSet rs = stmt.executeQuery();
			try {
				ResultSetMetaData meta = rs.getMetaData();  
				Map<String,Integer> columnsMap = buildColumnsMap(meta);
				DomainsLoadContext context = new DomainsLoadContext(columnsMap); 
				
				while (rs.next()) {
					DomainSpec domainSpec = createDomainSpec(rs, context);
					dbSchemaSpec.addDomainSpec(domainSpec);
					domainSpecByIdMap.put(rs.getInt(context.fieldID), domainSpec);
				}
			}
			finally {
				rs.close();
			}
			
		}
		finally {
			stmt.close();
		}
	}

	public static class TablesLoadContext {
		int id;
		int schema;
		int name;
		int title;
		int canAdd;
		int canEdit;
		int canDelete;
		int isExternal;
		int temporalMode;

		public TablesLoadContext(Map<String,Integer> columnsMap) {
			id = getColumnNo(columnsMap, "id");
			schema = getColumnNo(columnsMap, "schema_name");
			name = getColumnNo(columnsMap, "name");
			title = getColumnNo(columnsMap, "description");
			canAdd = getColumnNo(columnsMap, "can_add");
			canEdit = getColumnNo(columnsMap, "can_edit");
			canDelete = getColumnNo(columnsMap, "can_delete");
			isExternal = getColumnNo(columnsMap, "is_external");
			temporalMode = getColumnNo(columnsMap, "temporal_mode");
		}
	}
	
	protected static TableSpec createTableSpec(ResultSet rs, TablesLoadContext context,
			DbSchemaSpec dbSchemaSpec) throws SQLException {
		TableSpec result = new TableSpec(dbSchemaSpec);
		result.name = rs.getString(context.name);
		result.title = rs.getString(context.title);
		result.canAdd = rs.getBoolean(context.canAdd);
		result.canEdit = rs.getBoolean(context.canEdit);
		result.canDelete = rs.getBoolean(context.canDelete);
		String temporalMode = rs.getString(context.temporalMode);
		if (!rs.wasNull()) {
			result.temporalMode = temporalMode;
		}
		return result;
	}
	
	protected static void loadTableSpecs(Connection connection, DbSchemaSpec dbSchemaSpec,
			Map<Integer, TableSpec> tablesMapById) throws SQLException {
		PreparedStatement stmt = connection.prepareStatement("select "+
				"dbd$tables.id,"+
				"dbd$schemas.name as schema_name,"+
				"dbd$tables.name,"+
				"dbd$tables.description,"+
				"dbd$tables.can_add,"+
				"dbd$tables.can_edit,"+
				"dbd$tables.can_delete,"+
				"CASE WHEN NOT dbd$objects_attributes.id IS NULL THEN 1 END is_external, "+
				"dbd$tables.temporal_mode "+
				"FROM dbd$tables "+
				"LEFT JOIN dbd$schemas On dbd$tables.schema_id = dbd$schemas.id "+
				"LEFT JOIN dbd$objects_attributes ON "+
				"    (dbd$objects_attributes.object_id = dbd$tables.id) AND "+
				"    (dbd$objects_attributes.attribute_id = (SELECT id FROM dbd$dict_attributes WHERE guid = '"+ DBD_ATTR_EXTERNAL_TABLE +"'))");
		try {
			ResultSet rs = stmt.executeQuery();
			try {
				ResultSetMetaData meta = rs.getMetaData();  
				Map<String,Integer> columnsMap = buildColumnsMap(meta);
				TablesLoadContext context = new TablesLoadContext(columnsMap);
				TableSpec tableSpec;
				while (rs.next()) {
					if (rs.getBoolean(context.isExternal)) {
						// Если таблица является внешней (не описана в данном описателе), то она должна
						// быть в dbSchemaSpec (загружена ранее из предыдущего описателя)
						String tableName = rs.getString(context.name);
						tableSpec = dbSchemaSpec.findTableSpec(tableName);
						Preconditions.checkNotNull(tableSpec, "Не найден описатель внешней таблицы " + tableName);
					} else {
						tableSpec = createTableSpec(rs, context, dbSchemaSpec);
						dbSchemaSpec.addTableSpec(tableSpec);
					}
					tablesMapById.put(rs.getInt(context.id), tableSpec);
				}
			}
			finally {
				rs.close();
			}
		}
		finally {
			stmt.close();
		}
	}
	
	
	public static class FieldsLoadContext {
		int id;
		int domainId;
		int fieldName;
		int russianShortName;
		int fieldDescription;
		int canInput;
		int canEdit;
		int showInGrid;
		int showInDetails;
		int isMean;
		int autoAssignable;
		int isNotNull;
		int tableId;
		
		public FieldsLoadContext(Map<String,Integer> columnsMap) {
			 id = getColumnNo(columnsMap, "id");
			 domainId = getColumnNo(columnsMap, "domain_id");
		     fieldName = getColumnNo(columnsMap, "name");
		     russianShortName = getColumnNo(columnsMap, "russian_short_name");
		     fieldDescription = getColumnNo(columnsMap, "description");
		     canInput = getColumnNo(columnsMap, "can_input");
		     canEdit = getColumnNo(columnsMap, "can_edit");
		     showInGrid = getColumnNo(columnsMap, "show_in_grid");
		     showInDetails = getColumnNo(columnsMap, "show_in_details");
		     isMean = getColumnNo(columnsMap, "is_mean");
		     autoAssignable = getColumnNo(columnsMap, "autocalculated");
		     isNotNull = getColumnNo(columnsMap, "required");
		     tableId = getColumnNo(columnsMap, "table_id");
		}	
		
	}
	
	protected static FieldSpec createFieldToTableSpec(ResultSet rs, FieldsLoadContext context,
			Map<Integer, DomainSpec> domainsMapById, TableSpec tableSpec) throws SQLException {
		FieldSpec result = new FieldSpec(tableSpec);
		int domainId = rs.getInt(context.domainId);
		result.domainSpec = domainsMapById.get(domainId);
		Preconditions.checkNotNull(result.domainSpec,"Не найден домен для идентификатора "+domainId);
		result.name = rs.getString(context.russianShortName);
		result.latinName = rs.getString(context.fieldName);
		result.description = rs.getString(context.fieldDescription);
		result.canInput = rs.getBoolean(context.canInput);
		result.canEdit = rs.getBoolean(context.canEdit);
		result.showInGrid = rs.getBoolean(context.showInGrid);
		result.showInDetails = rs.getBoolean(context.showInDetails);
		result.isMean = rs.getBoolean(context.isMean);
		result.autoAssignable = rs.getBoolean(context.autoAssignable);
		result.isNotNull = rs.getBoolean(context.isNotNull);
		return result;
	}
	
	protected static void loadFieldSpecs(Connection connection, DbSchemaSpec dbSchemaSpec,
			Map<Integer, DomainSpec> domainsMapById,
			Map<Integer, TableSpec> tablesMapById,
			Map<Integer, FieldSpec> fieldSpecMapById) throws SQLException  {
		PreparedStatement stmt = connection.prepareStatement("select * from dbd$fields order by table_id, position");
		try {
			ResultSet rs = stmt.executeQuery();
			try {
				ResultSetMetaData meta = rs.getMetaData();  
				Map<String,Integer> columnsMap = buildColumnsMap(meta);
				FieldsLoadContext context = new FieldsLoadContext(columnsMap);
				Integer prevTableId = null;
				TableSpec tableSpec = null;
				while (rs.next()) {
					int tableId = rs.getInt(context.tableId);
					if (!((prevTableId != null) && (prevTableId == tableId))) {
						tableSpec = tablesMapById.get(tableId);
					}
					if (tableSpec != null) {
						FieldSpec fieldSpec  = createFieldToTableSpec(rs, context, domainsMapById, tableSpec);
						tableSpec.items.add(fieldSpec);
						fieldSpecMapById.put(rs.getInt(context.id), fieldSpec);

					}
					prevTableId = tableId;
				}
			}
			finally {
				rs.close();
			}
			
			for (int i = 0; i < dbSchemaSpec.getTableSpecCount(); i++) {
				dbSchemaSpec.getTableSpec(i).buildMap();
			}
		}
		finally {
			stmt.close();
		}
	}
	
	
	public static class IndexesLoadContext {
		int id;
		int name;
		int kind;
		int fieldId;
		int expression;
		int descend;
		int tableId;
		
		public IndexesLoadContext(Map<String,Integer> columnsMap) {
			 id = getColumnNo(columnsMap, "id");
			 name = getColumnNo(columnsMap, "name");
			 kind = getColumnNo(columnsMap, "kind");
			 fieldId = getColumnNo(columnsMap, "field_id");
			 expression = getColumnNo(columnsMap, "expression");
			 descend = getColumnNo(columnsMap, "descend");
			 tableId = getColumnNo(columnsMap, "table_id");
		}
	}
	
	protected static IndexType indexTypeByString(String value) {
		switch (value) {
			case "U":
				return IndexType.UNIQUE;
			case "N":
				return IndexType.NONUNIQUE;
			case "T":
				return IndexType.FULLTEXT;
			default:
				return IndexType.NONE;
		}
		
	}
	
	protected static IndexSpec createIndexSpec(ResultSet rs, IndexesLoadContext context) throws SQLException {
		IndexSpec result = new IndexSpec();
		result.name = rs.getString(context.name);
		result.indexType = indexTypeByString(rs.getString(context.kind));
		return result;
	}
	
	protected static void addIndexItemSpec(ResultSet rs, IndexesLoadContext context, IndexSpec indexSpec,
			Map<Integer, FieldSpec> fieldSpecMapById) throws SQLException {
		IndexItemSpec result = new IndexItemSpec(); 
		int fieldId = rs.getInt(context.fieldId);
		if (rs.wasNull()) {
			result.functionDesc = rs.getString(context.expression); 
		}
		else {
			result.fieldSpec = fieldSpecMapById.get(fieldId);
			Preconditions.checkNotNull(result.fieldSpec,"Не найдено поле для индекса по идентификатору "+fieldId);
		}
		if (rs.getBoolean(context.descend)) {
			result.indexOrder = IndexOrder.DESCEND; 
		}
		else {
			result.indexOrder = IndexOrder.ASCEND; 
		}
		indexSpec.items.add(result);
	}
	 
	protected static void loadIndexes(Connection connection, DbSchemaSpec dbSchemaSpec,
			Map<Integer, TableSpec> tablesMapById,
			Map<Integer, FieldSpec> fieldSpecMapById,
			Map<Integer, IndexSpec> indexSpecMapById) throws SQLException {
		PreparedStatement stmt = connection.prepareStatement("select * from dbd$indices "+
							"left join dbd$index_details on dbd$indices.id = dbd$index_details.index_id "+
							"order by dbd$indices.table_id, dbd$indices.id, dbd$index_details.position");
		try {
			ResultSet rs = stmt.executeQuery();
			try {
				ResultSetMetaData meta = rs.getMetaData();  
				Map<String,Integer> columnsMap = buildColumnsMap(meta);
				IndexesLoadContext context = new IndexesLoadContext(columnsMap);
				Integer prevTableId = null;
				Integer prevIndexId = null;
				TableSpec tableSpec = null;
				IndexSpec indexSpec = null;
				while (rs.next()) {
					int tableId = rs.getInt(context.tableId);
					int indexId = rs.getInt(context.id);
					if (!((prevTableId != null) && (prevTableId == tableId))) {
						tableSpec = tablesMapById.get(tableId);
					}
					if (tableSpec != null) {
						if (!((prevIndexId != null) && (prevIndexId == indexId))) {
							indexSpec = createIndexSpec(rs, context);
							tableSpec.indexItems.add(indexSpec);
							indexSpecMapById.put(indexId, indexSpec);
						}
						addIndexItemSpec(rs, context, indexSpec, fieldSpecMapById);
					}
					
					prevTableId = tableId;
					prevIndexId = indexId;
				}
			}
			finally {
				rs.close();
			}
			
		}
		finally {
			stmt.close();
			
		}
	}

	public static class ConstraintsLoadContext {
		int id;
		int name;
		int constraintType;
		int reference;
		int targetConstraintId;
		int hasValueEdit;
		int cascadingDelete;
		int expression;
		int fieldId;
		int tableId;
		
		public ConstraintsLoadContext(Map<String,Integer> columnsMap) {
			 id = getColumnNo(columnsMap, "id");
			 name = getColumnNo(columnsMap, "name");
			 constraintType = getColumnNo(columnsMap, "constraint_type");
			 reference = getColumnNo(columnsMap, "reference");
			 targetConstraintId = getColumnNo(columnsMap, "unique_key_id");
			 hasValueEdit = getColumnNo(columnsMap, "has_value_edit");
			 cascadingDelete = getColumnNo(columnsMap, "cascading_delete");
			 expression = getColumnNo(columnsMap, "expression");
			 fieldId = getColumnNo(columnsMap, "field_id");
			 tableId = getColumnNo(columnsMap, "table_id");
		}
	}
	
	protected static ConstraintSpec createConstraintSpec(ResultSet rs, ConstraintsLoadContext context) throws SQLException {
		String constraintType = rs.getString(context.constraintType);
		ConstraintSpec result = null;
		if (constraintType.equals("F") || constraintType.equals("R")) {
			ForeignKeyConstraintSpec foreignConstraint = new ForeignKeyConstraintSpec(); 
			foreignConstraint.foreignConstraintKind = constraintType;
			result = foreignConstraint;
			result.constraintType = ConstraintType.FOREIGN;
			foreignConstraint.hasValueEdit = rs.getBoolean(context.hasValueEdit);
			foreignConstraint.useCascadeDelete = rs.getBoolean(context.cascadingDelete);
		}
		else if (constraintType.equals("P")) {
			result = new PrimaryKeyConstraintSpec();
			result.constraintType = ConstraintType.PRIMARY;
		}
		else if (constraintType.equals("U")) {
			result = new UniqueKeyConstraintSpec();
			result.constraintType = ConstraintType.UNICAL;
			
		}
		else if (constraintType.equals("C")) {
			CheckConstraintSpec checkSpec = new CheckConstraintSpec();
			result = checkSpec;
			result.constraintType = ConstraintType.CHECK;
			checkSpec.expression = rs.getString(context.expression);
		}
		else {
			Preconditions.checkArgument(false,"Неизвестное ограничение "+constraintType);
		}
		result.name = rs.getString(context.name);
		return result;
	}
	
	protected static void loadConstraints(Connection connection, DbSchemaSpec dbSchemaSpec,
			Map<Integer, TableSpec> tablesMapById,
			Map<Integer, FieldSpec> fieldSpecMap,
			Map<Integer, ConstraintSpec> constraintSpecMap) throws SQLException {
		PreparedStatement stmt = connection.prepareStatement("select * from dbd$constraints "+
				"left join dbd$constraint_details on dbd$constraints.id = dbd$constraint_details.constraint_id "+
				"order by dbd$constraints.table_id, dbd$constraints.id, dbd$constraint_details.position");
		try {
			ResultSet rs = stmt.executeQuery();
			try {
				ResultSetMetaData meta = rs.getMetaData();  
				Map<String,Integer> columnsMap = buildColumnsMap(meta);
				ConstraintsLoadContext context = new ConstraintsLoadContext(columnsMap);

				Integer prevTableId = null;
				TableSpec tableSpec = null;
				Map<ForeignKeyConstraintSpec, Integer> foreignKeysMap = new HashMap<ForeignKeyConstraintSpec, Integer>(); 
				Integer prevConstraintId = null;
				ConstraintSpec constraintSpec = null;
				
				while (rs.next()) {
					int tableId = rs.getInt(context.tableId);
					int constraintId = rs.getInt(context.id);

					if (!((prevTableId != null) && (prevTableId == tableId))) {
						tableSpec = tablesMapById.get(tableId);
					}
					if (tableSpec != null) {
						if (!((prevConstraintId != null) && (prevConstraintId == constraintId))) {
							constraintSpec = createConstraintSpec(rs, context);
						    if (constraintSpec instanceof ForeignKeyConstraintSpec) {
								foreignKeysMap.put((ForeignKeyConstraintSpec) constraintSpec, rs.getInt(context.reference));
							}
							else if (constraintSpec instanceof PrimaryKeyConstraintSpec) {
								tableSpec.primaryKey = (PrimaryKeyConstraintSpec) constraintSpec; 
							}
							tableSpec.constraintItems.add(constraintSpec);
							constraintSpec.tableSpec = tableSpec;
						}
						if (constraintSpec instanceof ConstraintWithDetailsSpec) {
							int fieldIdent = rs.getInt(context.fieldId);
							FieldSpec constraintItem = fieldSpecMap.get(fieldIdent); 
							Preconditions.checkNotNull(constraintItem, "Не найдено поле с идентификатором "+fieldIdent);
							((ConstraintWithDetailsSpec)constraintSpec).items.add(constraintItem);
						}
						constraintSpecMap.put(constraintId, constraintSpec);
					}
					prevConstraintId = constraintId;
					prevTableId = tableId;
				}
				
				for (Map.Entry<ForeignKeyConstraintSpec, Integer> entry: foreignKeysMap.entrySet()) {
					TableSpec primaryTableSpec = tablesMapById.get(entry.getValue());
					Preconditions.checkNotNull(primaryTableSpec, "Не найдена таблица с идентификатором "+entry.getValue());
					ForeignKeyConstraintSpec foreignSpec = entry.getKey();
					foreignSpec.targetConstraint = primaryTableSpec.getPrimaryKey();
					Preconditions.checkNotNull(foreignSpec.targetConstraint, "Не найден первичный ключ таблицы "+primaryTableSpec.name);
				}
				
			}
			finally {
				rs.close();
			}
		}
		finally {
			stmt.close();
		}
		
	}
	
	protected static void addUsedAdditionsAttr(Connection connection, 
			DbSchemaSpec dbSchemaSpec) throws SQLException {
		/*  Если нет атрибута, содержащего список GUID'ов дополнений,
				включённых в описатель, то создадим его на основе записи
				с key='dbd.specific' из dbd$settings.
		    TODO: Удалить эту процедуру, когда данный атрибут будет автоматически 
				помещаться в описатель при его создании. */
		if (dbSchemaSpec.getAttributes().hasAttribute(DBD_ATTR_USED_ADDITIONS_ATTR_GUID)) {
			return;
		}
		final String DELIMITER = ",";
		HashSet<String> usedAdditionsXmlNames = new HashSet<>(Arrays.asList(
				getDbdSettingByKey(connection, DBD_SPECIFIC).split(DELIMITER)));
		ArrayList<String> guids = new ArrayList<>();
		try (PreparedStatement stmt = connection.prepareStatement("select guid, xml_name from dbd$dict_attributes")) {
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					if (usedAdditionsXmlNames.contains(rs.getString("xml_name"))) {
						guids.add(rs.getString("guid"));
					}
				}				
			}					
		}
		String attrValue = String.join(DELIMITER, guids);
		dbSchemaSpec.getAttributes().addAttribute(DBD_ATTR_USED_ADDITIONS_ATTR_GUID, attrValue);
	}

	protected static void AddAttribute(ResultSet rs, 
			AttributesLoadContext context, 
			DbdItemWithAttributes dbdItemSpec) throws SQLException {
		final String VALUE_FORBIDDEN = "F";
		final String VALUE_REQUIRED = "R";
		final String VALUE_ALLOWED = "A";

		String attrName = rs.getString(context.guid);
		String valueRequirements = rs.getString(context.valueRequirements);
		Preconditions.checkArgument(
				valueRequirements.equals(VALUE_FORBIDDEN) || valueRequirements.equals(VALUE_REQUIRED) || valueRequirements.equals(VALUE_ALLOWED),
				String.format("Атрибут '%1s' имеет неизвестный флаг обязательности значения '%2s'.", attrName, valueRequirements));
		String valueStr = rs.getString(context.value);
		if (rs.wasNull()) {
			valueStr = null;
		}
		String blobValue = rs.getString(context.blobValue);
		if (rs.wasNull()) {
			blobValue = null;
		}            
		if (valueRequirements.equals(VALUE_FORBIDDEN)) {
			Preconditions.checkState((valueStr == null) && (blobValue == null), "Атрибут '%s' не должен иметь значений.", attrName);
			dbdItemSpec.getAttributes().addAttribute(attrName);
		} else {
			FieldTypeId valueTypeID = FieldTypeId.fromString(rs.getString(context.valueType));
			String attrValue;
			if (SDataTypes.isBlobDataType(valueTypeID)) {
				attrValue = blobValue;
			} else {
				attrValue = valueStr;
			}
			if (attrValue != null) {
				dbdItemSpec.getAttributes().addAttribute(attrName, SDataTypes.convertStrToObjectByTID(attrValue, valueTypeID));
			} else {
				Preconditions.checkArgument(valueRequirements.equals(VALUE_REQUIRED), "Атрибут '%s' отмечен как обязательный, но его значение не задано.", attrName);
				dbdItemSpec.getAttributes().addAttribute(attrName);
			}                
		}
	}

	protected static void loadAttributes(Connection connection,
			DbSchemaSpec dbSchemaSpec,
			Map<Integer, DomainSpec> domainsMapById,
			Map<Integer, TableSpec> tablesMapById,
			Map<Integer, FieldSpec> fieldSpecMapById,
			Map<Integer, IndexSpec> indexSpecMapById, 
			Map<Integer, ConstraintSpec> constraintSpecMap, boolean isMainDBD) throws SQLException {
		PreparedStatement stmt = connection.prepareStatement(
				"select " +
				"  dbd$dict_attributes.guid,  " +
				"  dbd$dict_attributes.value_requirements, " +
				"  dbd$data_types.type_id, " +
				"  dbd$objects_attributes.value, " +
				"  dbd$objects_attributes.valueb, " +
				"  dbd$objects_attributes.object_id, " +
				"  dbd$schema_objects.name       object_type_name " +
				"from dbd$objects_attributes " +
				"inner join dbd$dict_attributes on dbd$dict_attributes.id = dbd$objects_attributes.attribute_id " +
				"left join dbd$schema_objects on dbd$schema_objects.id = dbd$objects_attributes.object_type_id " +
				"left join dbd$data_types on dbd$data_types.id = dbd$dict_attributes.content_type " +
				"where " +
				"  dbd$dict_attributes.guid <> '" + DBD_ATTR_EXTERNAL_TABLE + "' " + 
				"order by dbd$objects_attributes.object_type_id, dbd$objects_attributes.object_id");
		try {
			ResultSet rs = stmt.executeQuery();
			try {
				ResultSetMetaData meta = rs.getMetaData();
				Map<String, Integer> columnsMap = buildColumnsMap(meta);
				AttributesLoadContext context = new AttributesLoadContext(columnsMap);
				DbdItemWithAttributes dbdItemSpec;
				while (rs.next()) {
					// получаем объект по типу объекта и его id
					String objectTypeName = rs.getString(context.objectTypeName);
					Integer objectId = rs.getInt(context.objectId);
					switch (objectTypeName){
						case "project":     if (!isMainDBD) {
												// не загружаем атрибуты схемы (проекта) из пользовательского описателя
												continue;
											}
											dbdItemSpec = dbSchemaSpec;
											break;
						case "dbd$domains": dbdItemSpec = domainsMapById.get(objectId);
											break;
						case "dbd$tables":  dbdItemSpec = tablesMapById.get(objectId);
											break;
						case "dbd$fields":  dbdItemSpec = fieldSpecMapById.get(objectId);
											break;
						case "dbd$constraints": dbdItemSpec = constraintSpecMap.get(objectId);
											break;
						case "dbd$indices": dbdItemSpec = indexSpecMapById.get(objectId);
											break;
						default:			dbdItemSpec = null;
											break;
					}					
					Preconditions.checkNotNull(dbdItemSpec, String.format("Не найден объект типа '%1s' с кодом %2d.", objectTypeName, objectId));
					AddAttribute(rs, context, dbdItemSpec);
				}
			} finally {
				rs.close();
			}
		} finally {
			stmt.close();
		}
		addUsedAdditionsAttr(connection, dbSchemaSpec);
	}

	public static class AttributesLoadContext {
		int guid;
		int valueRequirements;
		int valueType;
		int value;
		int blobValue;
		int objectTypeName;
		int objectId;

		public AttributesLoadContext(Map<String,Integer> columnsMap) {
			guid = getColumnNo(columnsMap, "guid");
			valueRequirements = getColumnNo(columnsMap, "value_requirements");
			valueType = getColumnNo(columnsMap, "type_id");
			value = getColumnNo(columnsMap, "value");
			blobValue = getColumnNo(columnsMap, "valueb");
			objectTypeName = getColumnNo(columnsMap, "object_type_name");
			objectId = getColumnNo(columnsMap, "object_id");
		}
	}

	public static void loadFromFileToDBSchemaSpec(String fileName, DbSchemaSpec dbSchemaSpec,
			boolean isMainDBD) throws ClassNotFoundException, SQLException {
		initDriver();
		Connection connection = DriverManager.getConnection(JDBC_SQLITE_PREFIX + fileName);		
		try {
			String version = getDbdSettingByKey(connection, DBD_VERSION);
			chechDbdVersionIsActual(version);
			
			Map<Integer, DomainSpec> domainsMap = new HashMap<Integer, DomainSpec>();
			loadDomainSpecs(connection, dbSchemaSpec, domainsMap);

			Map<Integer, TableSpec> tablesMap = new HashMap<Integer, TableSpec>();
			loadTableSpecs(connection, dbSchemaSpec, tablesMap);
			
			Map<Integer, FieldSpec> fieldSpecMap = new HashMap<Integer, FieldSpec>();
			loadFieldSpecs(connection, dbSchemaSpec, domainsMap, tablesMap, fieldSpecMap);
			
			Map<Integer, IndexSpec> indexSpecMap = new HashMap<Integer, IndexSpec>();
			loadIndexes(connection, dbSchemaSpec, tablesMap, fieldSpecMap, indexSpecMap);
                        
			Map<Integer, ConstraintSpec> constraintSpecMap = new HashMap<>();
			loadConstraints(connection, dbSchemaSpec, tablesMap, fieldSpecMap, constraintSpecMap);
			
			loadAttributes(connection, dbSchemaSpec, 
					domainsMap, tablesMap, fieldSpecMap, indexSpecMap, constraintSpecMap, isMainDBD);
			dbSchemaSpec.buildDetailsMap();			
		}
		finally {
			connection.close();
		}
	}
	
	public static DbSchemaSpec loadFromFile(String fileName) throws ClassNotFoundException, SQLException {
		DbSchemaSpec dbSchemaSpec = new DbSchemaSpec();
		loadFromFileToDBSchemaSpec(fileName, dbSchemaSpec, true);
		return dbSchemaSpec;
	}
	
	private static void initDriver() throws ClassNotFoundException {
		Class.forName(SQLITE_JDBC_DRIVER);
	}
	
}

/*
05.06.2017 11:00 А.В.Шаров
 P* Код задачи 43210. Поддержана загрузка пользовательского метаописателя.
*/