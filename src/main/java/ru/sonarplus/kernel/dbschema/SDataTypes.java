package ru.sonarplus.kernel.dbschema;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.ArrayUtils;

import static ru.sonarplus.kernel.dbschema.FieldTypeId.*;

public class SDataTypes {
    // Взято из SDataTypes.pas:
	private static FieldTypeId[] integerDataTypes = {tid_SMALLINT, tid_INTEGER, tid_WORD, tid_BYTE, tid_LARGEINT};
	private static FieldTypeId[] numericDataTypes = (FieldTypeId[]) ArrayUtils.addAll(new FieldTypeId[] {tid_FLOAT}, integerDataTypes);
    private static FieldTypeId[] textDataTypes = {tid_STRING, tid_MEMO};
    private static FieldTypeId[] dateTimeDataTypes = {tid_DATE, tid_TIME, tid_DATETIME};

	public SDataTypes() {

	}

	public static boolean isBlobDataType(FieldTypeId typeId) {
		return typeId == tid_MEMO;
	}

	public static boolean isDateTimeDataType(FieldTypeId typeId) {
	    return fieldTypeIdIs(typeId, dateTimeDataTypes);
    }

    public static boolean isTextDataType(FieldTypeId typeId) {
	    return fieldTypeIdIs(typeId, textDataTypes);
    }

	public static boolean isNumericDataType(FieldTypeId typeId) {
		return fieldTypeIdIs(typeId, numericDataTypes );
	}

    public static boolean fieldTypeIdIs(FieldTypeId typeId, FieldTypeId[] typeIds) {
        return ArrayUtils.contains(typeIds, typeId);
    }

	public static Object convertStrToObjectByTID(String value, FieldTypeId typeId) {
		switch (typeId) {
			case tid_STRING:
			case tid_MEMO:
				return value;
			case tid_BOOLEAN:
				return Boolean.valueOf(value);
			case tid_BYTE:
				return Integer.valueOf(value);
			default:
				Preconditions.checkArgument(false, "Конвертация строки в тип '%s' не реализована", typeId);
				return null;
		}
	}

	/* Физический размер данных в байтах - отправляемая клиенту информация о
	   колонках резултьтата выборки, необходимая для работы Delphi kbmMW.
	   Следовало бы, наверное, использовать значения размеров,
	   взятых из Delphi (2007),
	   Но, за некоторым исключением (FIXEDCHAR), размеры данных в java совпадают с таковыми в Delphi.
	*/
    public static int defaultTypeIDNatSize(FieldTypeId tid) {
        switch (tid) {
            case tid_UNKNOWN:
                return 0;
            case tid_STRING:
                return 255;
            case tid_SMALLINT:
                return Short.BYTES;
            case tid_INTEGER:
                return Integer.BYTES;
            case tid_WORD:
                return Short.BYTES; // Word.BYTES
            case tid_BOOLEAN:
                return Byte.BYTES;
            case tid_FLOAT:
                return Double.BYTES;

            case tid_DATE:
            case tid_TIME:
                return Double.BYTES;

            case tid_BYTE:
                return Byte.BYTES;

            default:
                return 0;
        }
    }

    public static int defaultTypeIDNatSize(String type) {
        FieldTypeId tid = FieldTypeId.fromString(type);
        if (tid == null)
            tid = tid_UNKNOWN;
        return defaultTypeIDNatSize(tid);
    }

}
