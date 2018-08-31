package ru.sonarplus.kernel.sqlobject.common_utils;

import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_BOOLEAN;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_BYTE;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_CODE;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_DATE;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_DATETIME;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_FLOAT;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_INTEGER;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_LARGEINT;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_SMALLINT;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_STRING;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_TIME;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_UNKNOWN;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_WORD;


public class ValuesSupport {

    public static final int SONAR_ZERO_YEAR = 1872;
    public static final LocalDate SONAR_ZERO_DATE = LocalDate.of(SONAR_ZERO_YEAR, Month.JANUARY, 01);
    public static final LocalTime SONAR_ZERO_TIME = LocalTime.of(00, 00, 00);
    public static final LocalDateTime SONAR_ZERO_DATETIME = LocalDateTime.of(SONAR_ZERO_DATE, SONAR_ZERO_TIME);

    public static class ValueException extends SqlObjectException {
        public ValueException(String message) {
            super(message);
        }
    }

    public static void checkValue(Object value) throws ValueException {
        if ( !(value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long ||
                value instanceof LocalDateTime || value instanceof Float || value instanceof Double ||
                value instanceof CodeValue || value instanceof String ||
                value.getClass() == byte[].class)
                )
            throw new ValueException("Неподдерживаемое значение " + value.getClass().getName());
    }

    public static FieldTypeId defineValueType(Object value) {
        checkNotNull(value);
        if (value.getClass() == byte[].class)
            // байтовый массив единичной длины как байт. наверное это #BAD#, т.к. здесь идём "на поводу" у Oracle.
            // произвольный байтовый массив - как tid_CODE, хотя у нас есть tid_BYTES/tid_ARRAY, так что тоже #BAD#
            return ((byte[])value).length == 1 ? tid_BYTE : tid_CODE;

        if (value.getClass() == CodeValue.class)
            return tid_CODE;

        if (value.getClass() == Boolean.class)
            return tid_BOOLEAN;

        if (value.getClass() == Double.class)
            return tid_FLOAT;

        if (value instanceof String)
            return tid_STRING;

        if (value.getClass() == Float.class)
            return tid_FLOAT;

        if (value.getClass() == Byte.class)
            return tid_BYTE;

        if (value.getClass() == Short.class)
            return tid_SMALLINT;

        if (value.getClass() == Integer.class)
            return tid_INTEGER;

        if (value.getClass() == Long.class)
            return tid_LARGEINT;

        if (value.getClass() == LocalDateTime.class)
            return tid_DATETIME;

        return tid_UNKNOWN;
    }

    protected static int word(FieldTypeId fromType, Number value) {

        long v = value.longValue();
        if (value.getClass() == Byte.class || fromType == tid_BYTE)
            return (int) v & 0xFF;

        return (int) (value.longValue() & 0xFFFF);
    }

    public static Object castValue(Object sourceValue, FieldTypeId targetType)
            throws ValuesSupport.ValueException {
        return castValue(sourceValue, tid_UNKNOWN, targetType);
    }

    public static Object castValue(Object sourceValue, FieldTypeId sourceType, FieldTypeId targetType)
            throws ValuesSupport.ValueException {
        checkNotNull(sourceValue);
        switch (targetType) {
            case tid_CODE:
                if (sourceValue.getClass() == CodeValue.class)
                    return sourceValue;

                if (sourceValue instanceof String)
                    return CodeValue.valueOf((String) sourceValue);

                if (sourceValue instanceof Number)
                    return new CodeValue(new byte[] {((Number) sourceValue).byteValue()});

                if (sourceValue.getClass() == byte[].class)
                    return new CodeValue((byte[])sourceValue);

                throw new ValuesSupport.ValueException(sourceValue.getClass().getName() + "->" + targetType);

            case tid_BOOLEAN:
                if (sourceValue.getClass() == Boolean.class)
                    return sourceValue;

                if (sourceValue instanceof Number)
                    return ((Number)sourceValue).longValue() != 0;

                if (sourceValue instanceof String)
                    return Boolean.valueOf((String) sourceValue);

                if (sourceValue.getClass() == byte[].class) {
                    byte[] bytes = (byte[]) sourceValue;
                    return bytes.length != 0 && bytes[0] !=0;
                }

                throw new ValuesSupport.ValueException(sourceValue.getClass().getName() + "->" + targetType);

            case tid_STRING:
            case tid_MEMO:
                if (sourceValue instanceof String)
                    return sourceValue;
                return sourceValue.toString();

            case tid_FLOAT:
                if (sourceValue instanceof Number)
                    return sourceValue;

                if (sourceValue instanceof String)
                    return Double.valueOf((String) sourceValue);

                throw new ValuesSupport.ValueException(sourceValue.getClass().getName() + "->" + targetType);

            case tid_BYTE:
                if (sourceValue.getClass() == Byte.class)
                    return sourceValue;

                if (sourceValue instanceof String) {
                    return Integer.valueOf((String) sourceValue).byteValue();
                }

                if (sourceValue instanceof Number)
                    return ((Number)sourceValue).byteValue();

                if (sourceValue.getClass() == byte[].class) {
                    byte[] bytes = (byte[]) sourceValue;
                    return bytes.length != 0 ? bytes[0] : 0;
                }

                throw new ValuesSupport.ValueException(sourceValue.getClass().getName() + "->" + targetType);

            case tid_SMALLINT:
                if (sourceValue.getClass() == Short.class)
                    return sourceValue;

                if (sourceValue instanceof String)
                    return Short.valueOf((String)sourceValue);

                if (sourceValue instanceof Number)
                    return ((Number)sourceValue).shortValue();

                throw new ValuesSupport.ValueException(sourceValue.getClass().getName() + "->" + targetType);

            case tid_WORD:
                if (sourceValue instanceof Number)
                    return word(sourceType, ((Number)sourceValue).intValue());
                else if (sourceValue instanceof String)
                    return word(sourceType, Long.valueOf((String)sourceValue));

                throw new ValuesSupport.ValueException(sourceValue.getClass().getName() + "->" + targetType);

            case tid_INTEGER:
                if (sourceValue.getClass() == Integer.class)
                    return sourceValue;

                if (sourceValue instanceof String)
                    return Integer.valueOf((String)sourceValue);

                if (sourceValue instanceof Number)
                    return ((Number)sourceValue).intValue();

                throw new ValuesSupport.ValueException(sourceValue.getClass().getName() + "->" + targetType);

            case tid_LARGEINT:
                if (sourceValue.getClass() == Long.class)
                    return sourceValue;

                if (sourceValue instanceof String)
                    return Long.valueOf((String)sourceValue);

                if (sourceValue instanceof Number)
                    return ((Number)sourceValue).longValue();

                throw new ValuesSupport.ValueException(sourceValue.getClass().getName() + "->" + targetType);

            case tid_DATE:
                return toSonarDate(sourceValue);

            case tid_TIME:
                return toSonarTime(sourceValue);

            case tid_DATETIME:
                return toSonarDateTime(sourceValue);

            case tid_BLOB:
                if (sourceValue instanceof byte[])
                    return sourceValue;
                throw new ValuesSupport.ValueException(sourceValue.getClass().getName() + "->" + targetType);

            case tid_UNKNOWN:
                return null;

            default:
                throw new ValuesSupport.ValueException(sourceValue.getClass().getName() + "->" + targetType);
        }
    }

    protected static LocalDate toSonarDate(Object value) {
        if (value instanceof  String)
            return parseDate((String) value);
        if (value instanceof Number)
            return unixTimeToDateTime(value).toLocalDate();

        if (value instanceof LocalDate) {
            return (LocalDate) value;
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalDate();
        }
        throw new ValuesSupport.ValueException(value.getClass().getName() + "->" + tid_DATE);
    }

    protected static LocalTime toSonarTime(Object value) {
        if (value instanceof  String)
            return parseTime((String) value);
        if (value instanceof Number)
            return unixTimeToDateTime(value).toLocalTime();

        if (value instanceof LocalTime) {
            return (LocalTime) value;
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalTime();
        }
        throw new ValuesSupport.ValueException(value.getClass().getName() + "->" + tid_TIME);
    }

    protected static LocalDateTime toSonarDateTime(Object value)
            throws ValueException{
        if (value instanceof  String)
            return parseDateTime((String) value);
        if (value instanceof Number)
            return unixTimeToDateTime(value);

        if (!(value instanceof LocalDateTime))
            throw new ValuesSupport.ValueException(value.getClass().getName() + "->" + tid_DATETIME);
        return (LocalDateTime) value;
    }

    public static LocalDate parseDate(String value, String format) {
        return LocalDate.parse(value, DateTimeFormatter.ofPattern(StringUtils.isEmpty(format) ? "dd.MM.yyyy" : format));
    }

    public static LocalDate parseDate(String value) {
        return parseDate(value, null);
    }

    public static LocalTime parseTime(String value, String format) {
        return LocalTime.parse(value, DateTimeFormatter.ofPattern(StringUtils.isEmpty(format) ? "HH:mm:ss" : format));
    }

    public static LocalTime parseTime(String value) {
        return parseTime(value, null);
    }

    public static LocalDateTime parseDateTime(String value, String format) {
        return LocalDateTime.parse(value, DateTimeFormatter.ofPattern(StringUtils.isEmpty(format) ? "dd.MM.yyyy HH:mm:ss" : format));
    }

    public static LocalDateTime parseDateTime(String value) {
        return parseDateTime(value, null);
    }

    /**
     * Преобразует дату, время или дату-время в Unix-время (в милисекундах)
     * @param value - LocalDate или LocalTime или LocalDateTime, которое нужно преобразовать в Unix-время
     * @return
     */
    public static long dateTimeToUnixTime(Object value) {
        LocalDateTime dateTime = null;
        if (value instanceof LocalDate) {
            dateTime = LocalDateTime.of((LocalDate) value, LocalTime.MIDNIGHT);
        } else if (value instanceof LocalTime) {
            dateTime = LocalDateTime.of(LocalDate.ofEpochDay(0), (LocalTime) value);
        } else if (value instanceof LocalDateTime) {
            dateTime = (LocalDateTime) value;
        } else checkArgument(false);
        return dateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
    }

    public static LocalDateTime unixTimeToDateTime(Object value) {
        checkArgument(value instanceof Number);
        return Instant.ofEpochMilli(((Number)value).longValue()).atZone(ZoneId.of("UTC")).toLocalDateTime();
    }

    private static Map<FieldTypeId, Object> zeroValues = new HashMap<>();
    static {
        zeroValues.put(tid_BOOLEAN, Boolean.FALSE);
        zeroValues.put(tid_FLOAT, (float) 0);
        zeroValues.put(tid_CODE, new CodeValue(new byte[]{0}));
        zeroValues.put(tid_BYTE, (byte) 0);
        zeroValues.put(tid_SMALLINT, (short) 0);
        zeroValues.put(tid_INTEGER, 0);
        zeroValues.put(tid_LARGEINT, 0L);
        zeroValues.put(tid_DATE, SONAR_ZERO_DATE);
        zeroValues.put(tid_TIME, SONAR_ZERO_TIME);
        zeroValues.put(tid_DATETIME, SONAR_ZERO_DATETIME);
        zeroValues.put(tid_STRING, " ");
        zeroValues.put(tid_WORD, 0);
    }

    public static Object getZeroValue(FieldTypeId valueType)
            throws ValueException{
        Object value = zeroValues.get(valueType);
        if (value == null)
            throw new ValuesSupport.ValueException(String.format("Для типа %s не поддержано нулевое значение", valueType.toString()));
        return value;
    }
}
