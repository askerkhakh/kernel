package ru.sonarplus.kernel.sqlobject.distillation_old;

import ru.sonarplus.kernel.dbschema.DbSchemaSpec;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsDbSupportUtils;

public class DbcRec {
	public DbSchemaSpec schemaSpec;
	public SqlObjectsDbSupportUtils dbSupport;
	public boolean useStandardNulls;

	public DbcRec() {

	}
	
	public DbcRec(DbSchemaSpec schemaSpec, SqlObjectsDbSupportUtils dbSupport) {
		this.schemaSpec = schemaSpec;
		this.dbSupport = dbSupport;
		this.useStandardNulls = false;
	}

}
