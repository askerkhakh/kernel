package ru.sonarplus.kernel.request.param_converter;

import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;

import javax.inject.Named;
import javax.inject.Singleton;
import java.sql.Types;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDateTime;

public class ParamConverterPG extends ParamConverterDefault {

    public ParamConverterPG(boolean useStandardNulls) {
        super(useStandardNulls);
    }

    @Override
    public Object internalParamValueToJavaValue(Object value, FieldTypeId valueType) {
        switch (valueType) {
            case tid_BYTE:
                return ((Number)value).byteValue() & 0xFF;
        }
        return super.internalParamValueToJavaValue(value, valueType);
    }

}
