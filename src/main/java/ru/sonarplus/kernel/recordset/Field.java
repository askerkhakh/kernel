package ru.sonarplus.kernel.recordset;

import ru.sonarplus.kernel.dbschema.FieldSpec;

import javax.annotation.Nullable;

/**
 * Поле. Содержит значение и описатель поля.
 */
// TODO возможно следует разделить на два, т.к. поддержка OldValue нужна только для TableRecord, а для Record - не нужна.
// Кроме того, в простом Record у поля может не быть FieldSpec'а, а в TableRecord - нет.
// В Delphi DataSet представлял собой сборную солянку всего, но это не значит, что в Java это следует воспроизводить в
// точности и нельзя сделать лучше.
public interface Field {

    @Nullable
    FieldValue getValue();

    @Nullable
    FieldValue getOldValue();

    FieldSpec getFieldSpec();

    default String getName() {
        return getFieldSpec().getFieldName();
    }

    boolean isNull();

    void setValue(@Nullable FieldValue fieldValue);

    void setOldValue(@Nullable FieldValue oldFieldValue);
}
