package ru.sonarplus.kernel.dbd_attributes;

import ru.sonarplus.kernel.dbschema.FieldSpec;

public class VirtualField {
    public static boolean isFieldVirtual(FieldSpec fieldSpec) {
        return fieldSpec.getAttributes().hasAttribute("{D70C69A7-502A-4687-A7A2-41A6AD8FC04B}");
    }
}
