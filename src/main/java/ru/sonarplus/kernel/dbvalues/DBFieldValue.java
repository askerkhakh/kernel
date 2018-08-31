package ru.sonarplus.kernel.dbvalues;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.support.SqlLobValue;
import ru.sonarplus.kernel.dbschema.CodeTypeSpec;
import ru.sonarplus.kernel.dbschema.DataTypeSpec;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.SonarDbUtils;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.sql.Types;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;

import static ru.sonarplus.kernel.dbschema.FieldTypeId.*;
import static ru.sonarplus.kernel.dbschema.SDataTypes.*;

/**
 * Created by stepanov on 05.10.2017.
 */
public class DBFieldValue {

    public class ConvertDBFieldValueException extends Exception{
        public ConvertDBFieldValueException(String message) {
            super(message);
        }
    }

    public interface IEnumDBFieldValue {
        DBFieldValue toDBValue();
    }

    public static final FastDateFormat DATE_FORMAT = FastDateFormat.getInstance("dd/MM/yyyy");
    public static final FastDateFormat WORDS_DATE_FORMAT = FastDateFormat.getInstance("dd MMMM yyyy", new Locale("ru"));
    public static final FastDateFormat WORDS_DATE_FORMAT_NO_LEAD_NULLS = FastDateFormat.getInstance("d MMMM yyyy", new Locale("ru"));
    public static final FastDateFormat SHORT_DATE_FORMAT = FastDateFormat.getInstance("dd/MM/yy");
    public static final FastDateFormat TIME_FORMAT = FastDateFormat.getInstance("HH:mm:ss");
    public static final FastDateFormat DATE_TIME_FORMAT = FastDateFormat.getInstance("dd/MM/yyyy HH:mm:ss");

    @Nullable
    private Object value;
    public FieldTypeId typeId;
    @Nullable
    public DataTypeSpec typeSpec;

    private DBFieldValue(@Nullable Object value, FieldTypeId typeId, @Nullable DataTypeSpec typeSpec) {
        this.value = value;
        this.typeId = typeId;
        this.typeSpec = typeSpec;
    }

    public static DBFieldValue fromJavaValue(@Nullable Object value, DataTypeSpec typeSpec) {
        return new DBFieldValue(value, typeSpec.getFieldTypeId(), typeSpec);
    }
    
    public static DBFieldValue fromJavaValue(Object value, FieldTypeId typeId, DataTypeSpec typeSpec) {
        return new DBFieldValue(value, typeId, typeSpec);
    }

    public static DBFieldValue ofString(String value) {
        return new DBFieldValue(value, tid_STRING, null);
    }

    public static DBFieldValue ofDate(LocalDate date) {
        return new DBFieldValue(date, tid_DATE, null);
    }

    public static DBFieldValue ofTime(LocalTime time) {
        return new DBFieldValue(time, tid_TIME, null);
    }

    public static DBFieldValue ofLong(Long value) {
        return new DBFieldValue(value, tid_LARGEINT, null);
    }

    public static DBFieldValue fromDB(@Nullable Object value, FieldTypeId typeId, @Nullable DataTypeSpec typeSpec) {
        // в нашей БД вместо null хранится строка из одного пробела
        // надо ее конвертировать в настоящую пустую строку
        if (((typeId == FieldTypeId.tid_STRING) || (typeId == FieldTypeId.tid_MEMO)) && value instanceof String &&
                value.equals(" ")) {
            value = "";
        }
        return new DBFieldValue(value, typeId, typeSpec);
    }

    @Nullable
    public Object getValue() {
        return value;
    }

    public FieldTypeId getTypeId() {
        return typeId;
    }


    public void setNull() {
        value = null;
    }

    public DBFieldValue getClone() {
        return new DBFieldValue(value, typeId, typeSpec);
    }

    public boolean isNumeric() {
        if ((typeId != null) && (typeId != FieldTypeId.tid_UNKNOWN)) {
            return isNumericDataType(typeId);
        }
        return (value instanceof Number);
    }

    public boolean isBoolean() {
        return (value instanceof Boolean) ||
                typeId == tid_BOOLEAN;
    }

    public boolean isReal() {
        return value instanceof Float ||
                value instanceof Double ||
                (typeId == tid_FLOAT);
    }

    public boolean isDateTime() {
        return isDateTimeDataType(typeId) || (value instanceof Date);
    }

    protected boolean isRaw() {
        return (typeId == tid_BOOLEAN || typeId == tid_BYTE || typeId == tid_CODE);
    }

    private static String convertErrorMessage(Object value, FieldTypeId srcTypeId, String destType) {
        return String.format("Невозможно конвертировать значение %s типа %s в %s", value, srcTypeId, destType);
    }

    public Number asNumber() throws ConvertDBFieldValueException, ParseException {
        if (typeId == tid_BYTE && value instanceof byte[]) {
            if (((byte[]) value).length > 0) {
                byte srcValue = ((byte[]) value)[0];
                return srcValue >= 0 ? srcValue : 256 + srcValue;
            } else
                return 0;
        } else if (value instanceof Number){
            return (Number) value;
        } else if (value instanceof String)
            return NumberFormat.getInstance().parse((String) value);
        else if (value == null) {
            return 0;
        }
        else
           throw new ConvertDBFieldValueException(convertErrorMessage(value, typeId, "число"));
    }

    public boolean asBoolean() throws ConvertDBFieldValueException {
        if (typeId == tid_BOOLEAN && value instanceof byte[]) {
            if (((byte[]) value).length > 0) {
                return (((byte[]) value)[0] != 0);
            } else
                return false;
        } else if (value instanceof Boolean){
            return (Boolean) value;
        } else if (value instanceof String)
            return Boolean.parseBoolean((String) value);
        else if (value == null)
            return false;
        else
            throw new ConvertDBFieldValueException(convertErrorMessage(value, typeId, "значение булевского типа"));
    }

    public double asDouble() throws Exception {
        return asNumber().doubleValue();
    }

    public int[] asCode() throws ConvertDBFieldValueException {
        // TODO typeSPec у нас тут есть так что можно и нормальной длины вернуть массив
        if (typeId == tid_CODE && value instanceof byte[]) {
            // в java byte беззнаковый, а в кодификаторе могут быть значения больше 127
            // для упрощения далнейшей работы перекодируем в масив int
            byte[] val = (byte[]) value;
            int[] res = new int[val.length];
            for (int i = 0; i < val.length; i++) {
                res[i] = val[i] & 0xff;
            }
            return res;
        } else if (value instanceof int[])
            return (int[]) value;
        else if (value == null)
            return new int[]{0};
        else
            throw new ConvertDBFieldValueException(convertErrorMessage(value, typeId, "кодификатор"));

    }

    public Date asDate() throws ConvertDBFieldValueException {
        Date res;
        if (value == null)
            return (Date)SonarDbUtils.DEF_DATE.clone();
        if ((typeId == tid_DATE || typeId == tid_DATETIME || typeId ==  tid_TIME) &&
                value instanceof Long) {
            // проблема в том что чтоб не было проблем если в java старые настройки летнего времени,
            // клиенту дата-время отдается без конвертации в локальную временную зону (как если бы у нас была зона  UTC)
            // и от клиента соответственно тоже время приходит такое же, поэтому пока для получения
            // значения поля использются extractors
            //  надо это учесть
            ZonedDateTime zd = ZonedDateTime.ofInstant(Instant.ofEpochMilli((Long) value), ZoneId.of("UTC"));
            int y; int m; int d; int h; int min; int sec;
            // для поля времени в БД может быть указана (по ошибке) дата не соответствующая нулевой
            if (typeId == tid_TIME) {
                y = SonarDbUtils.DEF_DATE.getYear()+ 1900;
                m = SonarDbUtils.DEF_DATE.getMonth();
                d = SonarDbUtils.DEF_DATE.getDate();
            } else {
                y = zd.getYear();
                m = zd.getMonthValue()-1;
                d = zd.getDayOfMonth();
                if (typeId == tid_DATE) {

                }
            }
            if (typeId == tid_DATE) {
                h = 0;
                min = 0;
                sec = 0;
            } else {
                h = zd.getHour();
                min = zd.getMinute();
                sec = zd.getSecond();
            }

            GregorianCalendar cal = new GregorianCalendar(
                    y, m, d,
                    h, min, sec);
            res = cal.getTime();
        }
        else if (value instanceof Date) {
            if (typeId == tid_TIME) {
                value = ((Date) value).clone();
                ((Date) value).setYear(SonarDbUtils.DEF_DATE.getYear());
                ((Date) value).setMonth(SonarDbUtils.DEF_DATE.getMonth());
                ((Date) value).setDate(SonarDbUtils.DEF_DATE.getDate());
            } else if (typeId == tid_DATE) {
                value = ((Date) value).clone();
                ((Date) value).setHours(0);
                ((Date) value).setMinutes(0);
                ((Date) value).setSeconds(0);
            }
            res = (Date)value;
        } else
            throw new ConvertDBFieldValueException(convertErrorMessage(value, typeId, "дату"));
        return res;
    }

    public byte[] asBinData() {
        if (value == null)
            return new byte[]{};
        else
            return (byte[]) value;
    }

    /**
     * Проверка что значения поля равно null или приравненому к null значению
     * @return true если value равно null или приравненому к null значению, иначе false
     */
    public boolean isEmpty() {
        if (isDateTime())
            try {
                return SonarDbUtils.isNullDate(asDate());
            } catch (ConvertDBFieldValueException e){
                return SonarDbUtils.isNull(value);
            }
        else
            return SonarDbUtils.isNull(value);
    }

    /**
     * Проверка является ли значение поля null-значением неизвестного типа поля
     * @return true если является, false иначе
     */
    public boolean isUnknownTypeNullValue() {
        return value == null && (typeId == null || typeId == tid_UNKNOWN);
    }

    /**
     * Возвращает текстовое представление значения поля (таблицы в БД)
     * @return текстовое представление значения
     */
    public String toString() {
        if (value == null) {
            return "";
        } else if (value instanceof String) {
            return (String) value;
        }
        else {
            try {
                switch (typeId) {
                   case tid_DATE:
                        return DATE_FORMAT.format(asDate());
                    case tid_TIME:
                        return TIME_FORMAT.format(asDate());
                    case tid_DATETIME:
                        return DATE_TIME_FORMAT.format(asDate());
                    case tid_BOOLEAN:
                       return String.valueOf(asBoolean());
                    case tid_BYTE:
                       return asNumber().toString();
                    case tid_CODE:
                        return codeToString(asCode(), typeSpec);
                }
            } catch (ConvertDBFieldValueException | ParseException e) {
                return String.valueOf(value);
            }
        }
        return String.valueOf(value);
    }

    private static String codeToString(int[] value, @Nullable DataTypeSpec typeSpec) {
        StringBuilder res = new StringBuilder();
        int level;
        int nullCount;
        if (!(typeSpec instanceof CodeTypeSpec)) {
            level = value.length;
            nullCount = 3;
        } else {
            level = ((CodeTypeSpec) typeSpec).bytesForCode;
            nullCount = ((CodeTypeSpec) typeSpec).posPerPart;
            if (nullCount <= 0) {
                nullCount = 3;
            }
        }
        for (int i = 0; i < level; i++) {
            String codePart;
            if (i < value.length) {
                codePart = String.valueOf(value[i]);
            } else {
                codePart = "0";
            }
            codePart = StringUtils.leftPad(codePart, nullCount, '0');
            res.append(codePart);
            if (i < level-1) {
                res.append('.');
            }
        }
        return res.toString();
    }

    @Nullable
    public Object asDBValue() throws Exception {
        if (isEmpty()) {
            if (isDateTime())
                return SonarDbUtils.DEF_DATE;
            else if (isTextDataType(typeId))
                return SonarDbUtils.DEF_STRING;
            else switch (typeId) {
                case tid_BOOLEAN:
                    return SonarDbUtils.DEF_RAW;
                case tid_BYTE:
                    return SonarDbUtils.DEF_RAW;
                case tid_CODE:
                    return SonarDbUtils.DEF_RAW;
            }
            if (isNumeric())
                 return SonarDbUtils.DEF_NUM;
            return value;
        } else {
            if (isDateTime())
                return asDate();
            else if (isRaw() && value instanceof Number)
                return new byte[]{((Number) value).byteValue()}; //TODO
            else if (typeId == tid_BLOB) {
                if (value instanceof InputStream) {
                   return new SqlParameterValue(Types.BLOB, new SqlLobValue((InputStream)value, ((InputStream)value).available()));
                }
            }
            return value;
        }
    }

}

