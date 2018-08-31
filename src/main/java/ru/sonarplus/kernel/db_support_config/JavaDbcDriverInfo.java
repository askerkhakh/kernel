package ru.sonarplus.kernel.db_support_config;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class JavaDbcDriverInfo {

    public static final String CLASS_ORACLE_JDBC_ORACLEDRIVER = "oracle.jdbc.OracleDriver";
    public static final String CLASS_ORG_SQLITE_JDBC = "org.sqlite.JDBC";
    public static final String CLASS_ORG_POSTGRESQL_DRIVER = "org.postgresql.Driver";

    public static final String DRIVER_ID_ORACLE = "oracle";
    public static final String DRIVER_ID_SQLITE = "sqlite";
    public static final String DRIVER_ID_POSTGRESQL = "postgres";
    public static final String DRIVER_ID_FIREBIRD = "firebird";

    private static final Map<String, String> driverClassToDriverIDMap = new HashMap<>();
    static{
        driverClassToDriverIDMap.put(CLASS_ORACLE_JDBC_ORACLEDRIVER, DRIVER_ID_ORACLE);
        driverClassToDriverIDMap.put(CLASS_ORG_SQLITE_JDBC, DRIVER_ID_SQLITE);
        driverClassToDriverIDMap.put(CLASS_ORG_POSTGRESQL_DRIVER, DRIVER_ID_POSTGRESQL);
        //driverClassToDriverIDMap.put("jdbc_fb", DRIVER_ID_FIREBIRD);
    }

    private JavaDbcDriverInfo() {}

    private static JavaDbcDriverInfo instance;

    private String driverClassName = null;

    public static void setDriverClassName(String driverClassInfo) {
        if (instance == null)
            instance = new JavaDbcDriverInfo();
        Assert.hasText(driverClassInfo);
        // один сервер приложений - одна БД - один драйвер jdbc-подключения
        Assert.isTrue(
                !StringUtils.hasText(instance.driverClassName) || instance.driverClassName.equals(driverClassInfo),
                String.format(
                        "Уже зарегистрирован драйвер jdbc-соединения '%s' отличный от вновь устанавливаемого '%s'",
                        instance.driverClassName,
                        driverClassInfo));
        instance.driverClassName = driverClassInfo;
    }

    public static String getDriverID() {
        final String NO_DRIVER_CLASS = "Отсутствует информация о драйвере jdbc-подключения";
        Assert.notNull(instance, NO_DRIVER_CLASS);
        Assert.hasText(instance.driverClassName, NO_DRIVER_CLASS);
        String result = driverClassToDriverIDMap.get(instance.driverClassName);
        Assert.hasText(result);
        return result;
    }
}
