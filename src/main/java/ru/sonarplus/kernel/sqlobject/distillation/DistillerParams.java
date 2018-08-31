package ru.sonarplus.kernel.sqlobject.distillation;

import ru.sonarplus.kernel.dbschema.DbSchemaSpec;
import ru.sonarplus.kernel.sqlobject.db_support.SqlObjectsDbSupportUtils;

import java.time.LocalDateTime;

public class DistillerParams {
    public DbSchemaSpec schemaSpec;
    public SqlObjectsDbSupportUtils dbSupport;
    public boolean useStandartNulls;
    public LocalDateTime fixedCurrentDateTime;

    public DistillerParams(DbSchemaSpec schemaSpec, SqlObjectsDbSupportUtils dbSupport, boolean useStandartNulls) {
        this.schemaSpec = schemaSpec;
        this.dbSupport = dbSupport;
        this.useStandartNulls = useStandartNulls;
    }

    public DistillerParams(DbSchemaSpec schemaSpec, SqlObjectsDbSupportUtils dbSupport, boolean useStandartNulls, LocalDateTime fixedCurrentDateTime) {
        this(schemaSpec, dbSupport, useStandartNulls);
        this.fixedCurrentDateTime = fixedCurrentDateTime;
    }

}
