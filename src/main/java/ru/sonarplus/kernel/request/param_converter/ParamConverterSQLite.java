package ru.sonarplus.kernel.request.param_converter;

import ru.sonarplus.kernel.dbschema.FieldTypeId;

import javax.inject.Named;
import javax.inject.Singleton;
import java.sql.Types;

public class ParamConverterSQLite extends ParamConverterDefault {
    public ParamConverterSQLite(boolean useStandardNulls) {
        super(useStandardNulls);
    }
}