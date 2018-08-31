package ru.sonarplus.kernel.sqlobject.objects;

import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

import java.util.regex.Pattern;

// источник данных для from/join

public abstract class Source extends SqlObject {

	public Source() { super(); }

	public Source(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}
  public abstract String getTable();
  
  public abstract void setTable(String value);
  
  public abstract String getSchema();

  public abstract void setSchema(String value);

  public String getFullTableName() {
	  String result = getTable();
	  String schema = getSchema();
	  if (!StringUtils.isEmpty(schema)) {
		  result = schema + "." + result;
	  }
	  return result;
  }
  
  public void setFullTableName(String value) {
	  String[] result = value.split(Pattern.quote("."));
	  String table = "";
	  String schema = "";
	  if (result.length == 2) {
		  table = result[1];
		  schema = result[0];
	  }
	  else {
		  table = result[0];
	  }
  	  setSchema(schema);
	  setTable(table);
  }
}
