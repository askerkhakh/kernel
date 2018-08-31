package ru.sonarplus.kernel.db_support_config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DBSupportConfig {
    private static final Logger LOG = LoggerFactory.getLogger(DBSupportConfig.class);

    private static final Map<String, String> driverIDToMetaDataSupportClassMap = new ConcurrentHashMap<>();
    private static final Map<String, String> driverIDToSessionSupportClassMap = new ConcurrentHashMap<>();
    private static final Map<String, String> driverIDToConvertorClassMap = new ConcurrentHashMap<>();
    private static final Map<String, String> driverIDToTableNotificationsSupportDBMap = new ConcurrentHashMap<>();
    private static final Map<String, String> driverIDToParamConverterClassMap = new ConcurrentHashMap<>();
    private static final Map<String, String> driverIdToFieldDataConvertorClassMap = new ConcurrentHashMap<>();
    private static final Map<String, String> driverIdToUserServiceSupportClassMap = new ConcurrentHashMap<>();

    //mvd-common
    private static final String CLASS_META_DATA_SUPPORT_ORA = "ru.sonarplus.tor.util.db.sql.meta_data_support.MetaDataSupportOra";
    private static final String CLASS_META_DATA_SUPPORT_SQLITE = "ru.sonarplus.tor.util.db.sql.meta_data_support.MetaDataSupportSQLite";
    private static final String CLASS_META_DATA_SUPPORT_POSTGRESQL = "ru.sonarplus.tor.util.db.sql.meta_data_support.MetaDataSupportPG";
    private static final String CLASS_META_DATA_SUPPORT_FIREBIRD = "ru.sonarplus.tor.util.db.sql.meta_data_support.MetaDataSupportFB";

    //mvd-common
    private static final String CLASS_SESSION_SUPPORT_ORA = "ru.sonarplus.tor.util.db.sql.session_support.SessionSupportOra";
    private static final String CLASS_SESSION_SUPPORT_SQLITE = "ru.sonarplus.tor.util.db.sql.session_support.SessionSupportSQLite";
    private static final String CLASS_SESSION_SUPPORT_POSTGRESQL = "ru.sonarplus.tor.util.db.sql.session_support.SessionSupportPG";

    //common-services
    private static final String CLASS_CONVERTOR_ORA = "ru.sonarplus.kernel.sqlobject.db_support_ora.SqlObjectsConvertorOra";
    private static final String CLASS_CONVERTOR_SQLITE = "ru.sonarplus.kernel.sqlobject.db_support_sqlite.SqlObjectsConvertorSQLite";
    private static final String CLASS_CONVERTOR_POSTGRESQL = "ru.sonarplus.kernel.sqlobject.db_support_pg.SqlObjectsConvertorPG";
    private static final String CLASS_CONVERTOR_FIREBIRD = "ru.sonarplus.kernel.sqlobject.db_support_fb.SqlObjectsConvertorFB";

    //common-services
    private static final String CLASS_NOTIFICATION_TABLE_SUPPORT_ORA = "ru.sonarplus.notifications_dbtools.db.NotificationsTableSupportOra";
    private static final String CLASS_NOTIFICATION_TABLE_SUPPORT_SQLITE = "ru.sonarplus.notifications_dbtools.db.NotificationsTableSupportSQLite";
    private static final String CLASS_NOTIFICATION_TABLE_SUPPORT_POSTGRESQL = "ru.sonarplus.notifications_dbtools.db.NotificationsTableSupportPG";

    //common-services
    private static final String CLASS_PARAM_CONVERTER_ORA = "ru.sonarplus.kernel.request.param_converter.ParamConverterOra";
    private static final String CLASS_PARAM_CONVERTER_SQLITE = "ru.sonarplus.kernel.request.param_converter.ParamConverterSQLite";
    private static final String CLASS_PARAM_CONVERTER_POSTGRESQL = "ru.sonarplus.kernel.request.param_converter.ParamConverterPG";

    //tor-server
    private static final String FIELD_VALUE_CONVERTOR_ORA = "ru.reksoft.sonar.tor.sql.field_data_convertor.FieldValueConvertorOra";
    private static final String FIELD_VALUE_CONVERTOR_SQLITE = "ru.reksoft.sonar.tor.sql.field_data_convertor.FieldValueConvertorSQLite";
    private static final String FIELD_VALUE_CONVERTOR_POSTGRESQL = "ru.reksoft.sonar.tor.sql.field_data_convertor.FieldValueConvertorPG";
    private static final String FIELD_VALUE_CONVERTOR_FIREBIRD = "ru.reksoft.sonar.tor.sql.field_data_convertor.FieldValueConvertorFB";

    //sonar-storage
    private final static String CLASS_USER_SERVICE_SUPPORT_ORA = "ru.sonarplus.user.user_service_support.UserServiceSupportOra";
    private final static String CLASS_USER_SERVICE_SUPPORT_SQLITE = "ru.sonarplus.user.user_service_support.UserServiceSupportSQLite";
    private final static String CLASS_USER_SERVICE_SUPPORT_POSTGRESQL = "ru.sonarplus.user.user_service_support.UserServiceSupportPG";

    static {
        driverIDToMetaDataSupportClassMap.put(JavaDbcDriverInfo.DRIVER_ID_ORACLE, CLASS_META_DATA_SUPPORT_ORA);
        driverIDToMetaDataSupportClassMap.put(JavaDbcDriverInfo.DRIVER_ID_SQLITE, CLASS_META_DATA_SUPPORT_SQLITE);
        driverIDToMetaDataSupportClassMap.put(JavaDbcDriverInfo.DRIVER_ID_POSTGRESQL, CLASS_META_DATA_SUPPORT_POSTGRESQL);
        driverIDToMetaDataSupportClassMap.put(JavaDbcDriverInfo.DRIVER_ID_FIREBIRD, CLASS_META_DATA_SUPPORT_FIREBIRD);

        driverIDToSessionSupportClassMap.put(JavaDbcDriverInfo.DRIVER_ID_ORACLE, CLASS_SESSION_SUPPORT_ORA);
        driverIDToSessionSupportClassMap.put(JavaDbcDriverInfo.DRIVER_ID_SQLITE, CLASS_SESSION_SUPPORT_SQLITE);
        driverIDToSessionSupportClassMap.put(JavaDbcDriverInfo.DRIVER_ID_POSTGRESQL, CLASS_SESSION_SUPPORT_POSTGRESQL);

        driverIDToConvertorClassMap.put(JavaDbcDriverInfo.DRIVER_ID_ORACLE, CLASS_CONVERTOR_ORA);
        driverIDToConvertorClassMap.put(JavaDbcDriverInfo.DRIVER_ID_SQLITE, CLASS_CONVERTOR_SQLITE);
        driverIDToConvertorClassMap.put(JavaDbcDriverInfo.DRIVER_ID_POSTGRESQL, CLASS_CONVERTOR_POSTGRESQL);
        driverIDToConvertorClassMap.put(JavaDbcDriverInfo.DRIVER_ID_FIREBIRD, CLASS_CONVERTOR_FIREBIRD);

        driverIDToTableNotificationsSupportDBMap.put(JavaDbcDriverInfo.DRIVER_ID_ORACLE, CLASS_NOTIFICATION_TABLE_SUPPORT_ORA);
        driverIDToTableNotificationsSupportDBMap.put(JavaDbcDriverInfo.DRIVER_ID_SQLITE, CLASS_NOTIFICATION_TABLE_SUPPORT_SQLITE);
        driverIDToTableNotificationsSupportDBMap.put(JavaDbcDriverInfo.DRIVER_ID_POSTGRESQL, CLASS_NOTIFICATION_TABLE_SUPPORT_POSTGRESQL);

        driverIDToParamConverterClassMap.put(JavaDbcDriverInfo.DRIVER_ID_ORACLE, CLASS_PARAM_CONVERTER_ORA);
        driverIDToParamConverterClassMap.put(JavaDbcDriverInfo.DRIVER_ID_SQLITE, CLASS_PARAM_CONVERTER_SQLITE);
        driverIDToParamConverterClassMap.put(JavaDbcDriverInfo.DRIVER_ID_POSTGRESQL, CLASS_PARAM_CONVERTER_POSTGRESQL);

        driverIdToFieldDataConvertorClassMap.put(JavaDbcDriverInfo.DRIVER_ID_ORACLE, FIELD_VALUE_CONVERTOR_ORA);
        driverIdToFieldDataConvertorClassMap.put(JavaDbcDriverInfo.DRIVER_ID_SQLITE, FIELD_VALUE_CONVERTOR_SQLITE);
        driverIdToFieldDataConvertorClassMap.put(JavaDbcDriverInfo.DRIVER_ID_POSTGRESQL, FIELD_VALUE_CONVERTOR_POSTGRESQL);
        driverIdToFieldDataConvertorClassMap.put(JavaDbcDriverInfo.DRIVER_ID_FIREBIRD, FIELD_VALUE_CONVERTOR_FIREBIRD);

        driverIdToUserServiceSupportClassMap.put(JavaDbcDriverInfo.DRIVER_ID_ORACLE, CLASS_USER_SERVICE_SUPPORT_ORA);
        driverIdToUserServiceSupportClassMap.put(JavaDbcDriverInfo.DRIVER_ID_SQLITE, CLASS_USER_SERVICE_SUPPORT_SQLITE);
        driverIdToUserServiceSupportClassMap.put(JavaDbcDriverInfo.DRIVER_ID_POSTGRESQL, CLASS_USER_SERVICE_SUPPORT_POSTGRESQL);
    }

    protected static Object createInstance(String driverId, Map<String, String> driverIdToClassNameMap, String classNotFoundMsg) {
        try {
            Assert.notNull(driverIdToClassNameMap);
            Assert.hasText(driverId, "Не указан идентификатор драйвера БД");
            String className = driverIdToClassNameMap.get(driverId);
            Assert.hasText(className, String.format("Не найден %s для драйвера '%s'", classNotFoundMsg, driverId));
            return Class.forName(className).newInstance();
        }
        catch (Exception e) {
            LOG.debug(String.valueOf(e));
            throw new RuntimeException(e);
        }
    }

    public static Object createMetaDataSupportInstance() {
        return createInstance(JavaDbcDriverInfo.getDriverID(), driverIDToMetaDataSupportClassMap, "класс поддержки метаданных");
    }

    public static Object createSessionSupportInstance() {
        return createInstance(JavaDbcDriverInfo.getDriverID(), driverIDToSessionSupportClassMap, "класс поддержки сеанса");
    }

    public static Object createConvertorInstance() {
        return createInstance(JavaDbcDriverInfo.getDriverID(), driverIDToConvertorClassMap, "класс конвертрера в sql");
    }

    public static Object createNotificationsSupportInstance() {
        return createInstance(JavaDbcDriverInfo.getDriverID(), driverIDToTableNotificationsSupportDBMap, "класс поддержки уведомлений");
    }

    public static Object createParamValueConverterInstance() {
        return createInstance(JavaDbcDriverInfo.getDriverID(), driverIDToParamConverterClassMap, "класс поддержки конвертации значений параметров");
    }

    public static Object createFieldDataConverterInstance() {
        return createInstance(JavaDbcDriverInfo.getDriverID(), driverIdToFieldDataConvertorClassMap, "класс поддержки конвертации значений полей");
    }

    public static Object createUserServiceSupportInstance() {
        return createInstance(JavaDbcDriverInfo.getDriverID(), driverIdToUserServiceSupportClassMap, "класс поддержки конвертации значений полей");
    }
}
