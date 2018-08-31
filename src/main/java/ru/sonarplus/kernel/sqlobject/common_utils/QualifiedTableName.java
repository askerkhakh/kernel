package ru.sonarplus.kernel.sqlobject.common_utils;
import java.util.regex.Pattern;

public class QualifiedTableName {
	public String schema;
	public String table;

	public QualifiedTableName(String schema, String table) {
		this.schema = schema;
		this.table = table;
	}
	
	public static QualifiedTableName stringToQualifiedTableName(String value) {
		String[] items = value.split(Pattern.quote("."));
		if (items.length == 2) {
			return new QualifiedTableName(items[0], items[1]);
		}
		else {
			return new QualifiedTableName("", items[0]);
		}
	}

}
