package ru.sonarplus.kernel;

import java.sql.Types;
import java.util.Calendar;
import java.util.Date;

import oracle.jdbc.OracleTypes;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

import javax.annotation.Nullable;

/**
 * Вспомогательный класс для работы с сонаровскими типами данных.
 * 
 * @author gubanov
 * 
 */
public final class SonarDbUtils {

	private static final int RAW_PADDING = 2;

	private static final int HEX_BASE = 16;

	private static final int MAX_BYTE_VALUE = 0xff;

	private static final int NULL_YEAR = 1872;

	private static final int[] NUM_TYPES = new int[] { Types.BIGINT, Types.INTEGER, Types.NUMERIC, Types.DECIMAL,
			Types.DOUBLE, Types.FLOAT, Types.SMALLINT, Types.TINYINT };
	private static final int[] DATE_TYPES = new int[] { Types.DATE, Types.TIMESTAMP, Types.TIME };
	private static final int[] STR_TYPES = new int[] { Types.VARCHAR, Types.CHAR, Types.CLOB, Types.LONGNVARCHAR,
			Types.LONGVARCHAR, Types.NCHAR, Types.NCLOB };
	private static final int[] BIN_TYPES = new int[] { Types.ARRAY, Types.BINARY, Types.NCLOB, Types.VARBINARY,
			OracleTypes.RAW };

	@SuppressWarnings("MS_PKGPROTECT")
	public static final byte[] TRUE = new byte[] { 1 };

	@SuppressWarnings("MS_PKGPROTECT")
	public static final byte[] FALSE = new byte[] { 0 };

	@SuppressWarnings("MS_PKGPROTECT")
	// NULL_RAW не должен быть == FALSE
	public static final byte[] NULL_RAW = new byte[] { 0 };

	@java.lang.SuppressWarnings("deprecation")
	public static final Date DEF_DATE = new Date(-28, 0, 1);

	public static final String DEF_STRING = " ";
	public static final Integer DEF_NUM = 0;
	public static final byte[] DEF_RAW = NULL_RAW;

	private SonarDbUtils() {

	}

	public static class DateTime {
		private Date date;
		private Date time;

		public Date getDate() {
			return date;
		}

		public void setDate(Date date) {
			this.date = date;
		}

		public Date getTime() {
			return time;
		}

		public void setTime(Date time) {
			this.time = time;
		}
	}

	/**
	 * Объединяет дату и время в одну дату.
	 * 
	 * @param d
	 *            дата ,содержащая год месяц и число
	 * @param t
	 *            дата содержащая часы минуты и секунды
	 * @return объединенная дата
	 */
	public static Date combine(Date d, Date t) {
		Calendar date = Calendar.getInstance();
		date.setTime(d);

		Calendar time = Calendar.getInstance();
		time.setTime(t);

		date.set(Calendar.HOUR_OF_DAY, time.get(Calendar.HOUR_OF_DAY));
		date.set(Calendar.MINUTE, time.get(Calendar.MINUTE));
		date.set(Calendar.SECOND, time.get(Calendar.SECOND));
		date.set(Calendar.MILLISECOND, time.get(Calendar.MILLISECOND));

		return date.getTime();
	}

	/**
	 * Расщипляет дату-время на дату (год месяц число) и время (часы минуты секунды)
	 * 
	 * @param datetime
	 *            дата-время
	 * @return дата и время по-отдельности
	 */
	public static DateTime split(Date datetime) {
		Calendar date = Calendar.getInstance();
		Calendar time = Calendar.getInstance();

		time.setTime(datetime);
		time.set(Calendar.DAY_OF_MONTH, 1);
		time.set(Calendar.YEAR, NULL_YEAR);
		time.set(Calendar.MONTH, 0);

		date.setTime(datetime);
		date.set(Calendar.HOUR_OF_DAY, 0);
		date.set(Calendar.MINUTE, 0);
		date.set(Calendar.SECOND, 0);
		date.set(Calendar.MILLISECOND, 0);

		DateTime result = new DateTime();
		result.setDate(date.getTime());
		result.setTime(time.getTime());

		return result;
	}

	/**
	 * В БД Сонар дата и время храняться отдельно, и то и другое хранится в поле типа DATE, этот метод позволяет
	 * определить, чем является value - датой (день месяц год), или временм (часы минуты секунды)
	 * 
	 * @param value
	 *            проверямое значение
	 * @return true если value - время, false если value - дата
	 */
	public static boolean isTime(Date value) {
		DateTime splited = split(value);
		return isNullDate(splited.date);
	}

	/**
	 * Преобразует массив байт в булево значение.
	 * 
	 * @param raw
	 *            массив байтов
	 * @return булево значение
	 */
	public static boolean isTrue(byte[] raw) {
		if (raw == null || raw.length == 0) {
			return false;
		}
		return raw[0] == 1;
	}

	/**
	 * Возвращает true если .
	 * 
	 * @param raw
	 *            массив байтов
	 * @return булево значение
	 */
	public static boolean isEmpty(byte[] raw) {
		return raw == null || (raw.length == 1 && raw[0] == 0);
	}

	/**
	 * Преобразует булево значение в массив байтов.
	 * 
	 * @param val
	 *            булево значение
	 * @return массив байтов
	 */
	@SuppressWarnings("MS_EXPOSE_REP")
	public static byte[] rawValue(boolean val) {
		return val ? TRUE : FALSE;
	}

	/**
	 * Является ли дата нулевой в терминах базы Сонар.
	 * 
	 * @param value
	 *            дата, которую проверяем на null
	 * @return true- является, false - нет
	 */
	public static boolean isNullDate(Date value) {
		return value == null || DEF_DATE.getTime() == value.getTime();
	}

	/**
	 * Является ли строка нулевой в терминах базы Сонар.
	 * 
	 * @param value
	 *            строка, которую проверяем на null
	 * @return true- является, false - нет
	 */
	public static boolean isNullStr(String value) {
		return value == null || DEF_STRING.equals(value) || value.equals("");
	}

	/**
	 * Является ли число нулевым в терминах базы Сонар.
	 * 
	 * @param value
	 *            число, которое проверяем на null
	 * @return true- является, false - нет
	 */
	public static boolean isNullNum(Number value) {
		return value == null || value.doubleValue() == 0.0;
	}

	/**
	 * Является ли поле RAW нулевым в терминах базы Сонар.
	 * 
	 * @param value
	 *            поле RAW, которое проверяем на null
	 * @return true- является, false - нет
	 */
	public static boolean isNullRaw(byte[] value) {
		return value == null || value.length == 1 && value[0] == 0;
	}

	/**
	 * Определяет является ли значение нулевым в теримнах Бд Сонар
	 * 
	 * @param value
	 *            значение
	 * @return true - является, false - не является
	 */
	public static boolean isNull(@Nullable Object value) {
		if (value == null) {
			return true;
		}
		if (value instanceof Date) {
			return isNullDate((Date) value);
		} else if (value instanceof String) {
			return isNullStr((String) value);
		} else if (value instanceof Number) {
			return isNullNum((Number) value);
		} else if (value instanceof byte[]) {
			return isNullRaw((byte[]) value);
		}
		return false;
	}

	/**
	 * Конвертирует тип данных raw в строку в формате Сонар.
	 * 
	 * @param raw
	 *            значение RAW поля
	 * @return строка в формате Сонар.
	 */
	public static String rawToString(byte[] raw) {
		if (raw == null) {
			return "null";
		}
		return rawToString(raw, raw.length);
	}

	/**
	 * Конвертирует тип данных raw в строку в формате Сонар.
	 * 
	 * @param raw
	 *            значение RAW поля
	 * @param minSize
	 *            минимальный выводимый размер, если raw.length < minSize результат будет дополнен нулями справа
	 * @return строка в формате Сонар.
	 */
	public static String rawToString(byte[] raw, int minSize) {
		if (raw == null) {
			return "null";
		}
		StringBuilder buf = new StringBuilder();
		for (byte level : raw) {
			buf.append(StringUtils.leftPad(String.valueOf(level & MAX_BYTE_VALUE), RAW_PADDING, '0')).append('.');
		}
		for (int i = raw.length; i < minSize; i++) {
			buf.append("00.");
		}
		// Убираем последнюю точку
		return buf.substring(0, buf.length() - 1);
	}

	/**
	 * Аналог Oracle функции hextoraw
	 * 
	 * @param hexString
	 *            строка - 16ричное число
	 * @return массив байт, такой же как в поле RAW Oracle, rawtohex которого равен hexString
	 */
	public static byte[] hexToRaw(String hexString) {
		String validHexString = hexString;
		if (validHexString.length() % 2 != 0) {
			validHexString = '0' + validHexString;
		}
		byte[] result = new byte[validHexString.length() / 2];
		for (int i = 0; i < validHexString.length(); i += 2) {
			result[i / 2] = (byte) Integer.parseInt(validHexString.substring(i, i + 2), HEX_BASE);
		}
		return result;
	}

	public static Object defaultValue(int sqlType) {
		Object result;
		if (isNumber(sqlType)) {
			result = 0;
		} else if (isDate(sqlType)) {
			result = SonarDbUtils.DEF_DATE;
		} else if (isString(sqlType)) {
			result = SonarDbUtils.DEF_STRING;
		} else if (isBinary(sqlType)) {
			result = SonarDbUtils.NULL_RAW;
		} else {
			throw new IllegalStateException("Не поддерживаемый тип данных: " + sqlType);
		}
		return result;
	}

	private static boolean isBinary(int type) {
		return ArrayUtils.contains(BIN_TYPES, type);
	}

	private static boolean isString(int type) {
		return ArrayUtils.contains(STR_TYPES, type);
	}

	private static boolean isDate(int type) {
		return ArrayUtils.contains(DATE_TYPES, type);
	}

	private static boolean isNumber(int type) {
		return ArrayUtils.contains(NUM_TYPES, type);
	}
}
