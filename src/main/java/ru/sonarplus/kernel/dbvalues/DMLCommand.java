package ru.sonarplus.kernel.dbvalues;

import org.apache.commons.lang3.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.sonarplus.kernel.dbschema.FieldTypeId;

import java.util.*;

/**
 * Created by stepanov on 29.11.2017.
 */
public class DMLCommand {
    private static final Logger LOG = LoggerFactory.getLogger(DMLCommand.class);

    public String command;
    public Object[] params;

    public DMLCommand(String command, Object[] params) {
        this.command = command;
        this.params = params;
    }

    public static String selectNextValCommand(String table, String schema) {
        String seqName = "SEQ_" + table;
        if (schema != null && !schema.isEmpty()) {
            seqName = schema + "." + seqName;
        }
        return "select "+ seqName + ".nextval from dual";
    }

    public static String tableNameWithSchema(String tableName, String schema) {
        if (schema != null && !schema.isEmpty())
            return schema + "." + tableName;
        else
            return tableName;
    }

    /**
     * Построение команды на добавление вида insert into ${TABLE} (${FIELDS}) from select (${VALUES}) from .. where ..
     * @param sql текст команды куда подставятся нужные значения
     * @param table имя таблицы ${TABLE}
     * @param fields список полей откуда нужно взять перечень имен и значения
     * @return DMCommand
     * @throws Exception
     */
    public static DMLCommand buildInsertFromSelect(String sql, String table, ADBField[] fields, Object... whereParams) throws Exception {
        StringBuilder fieldsClause = new StringBuilder();
        StringBuilder valuesClause = new StringBuilder();
        List<Object> params = new ArrayList<Object>();
        for (ADBField fld: fields) {
            if (!fld.getValue().isEmpty() || fld.sqlCommand != null) {
                fieldsClause.append(fld.lFieldName).append(',');
                if (fld.sqlCommand != null) {
                    valuesClause.append(fld.sqlCommand).append(",");
                    if (fld.sqlCommandParams != null)
                        params.addAll(fld.sqlCommandParams);
                } else {
                    DBFieldValue val = fld.getValue();
                    valuesClause.append("?,");
                    params.add(val.asDBValue());
                }
            }
        }
        // Удаляем лишние запятые в конце
        fieldsClause.setLength(fieldsClause.length() - 1);
        valuesClause.setLength(valuesClause.length() - 1);
        Map<String, String> substs = new HashMap<String, String>();
        substs.put("TABLE", table);
        substs.put("FIELDS", fieldsClause.toString());
        substs.put("VALUES", valuesClause.toString());
        StrSubstitutor subst = new StrSubstitutor(substs);
        String command = subst.replace(sql);
        if (whereParams != null) {
            params.addAll(Arrays.asList(whereParams));
        }
        return new DMLCommand(command, params.toArray());
    }

    public static DMLCommand buildInsertCommand(String table, String schema, ADBField[] fields) throws Exception {
        StringBuilder buf = new StringBuilder();
        if (schema !=null && !schema.isEmpty()) {
            table = schema + "." + table;
        }
        buf.append("INSERT INTO ").append(table);

        List<Object> params = new ArrayList<Object>();

        StringBuilder fieldsClause = new StringBuilder();
        StringBuilder valuesClause = new StringBuilder();
        String primaryKey = null;
        for (ADBField fld: fields) {
            if (!fld.getValue().isEmpty() || fld.sqlCommand != null) {
                fieldsClause.append(fld.lFieldName).append(',');
                if (fld.sqlCommand != null) {
                    valuesClause.append(fld.sqlCommand).append(",");
                    if (fld.sqlCommandParams != null)
                        params.addAll(fld.sqlCommandParams);
                } else {
                    valuesClause.append("?,");
                    params.add(fld.getValue().asDBValue());
                }
            }
        }
        // Удаляем лишние запятые в конце
        fieldsClause.setLength(fieldsClause.length() - 1);
        valuesClause.setLength(valuesClause.length() - 1);
        buf.append('(').append(fieldsClause).append(") VALUES (").append(valuesClause).append(')');
        return new DMLCommand(buf.toString(), params.toArray());
    }

    private static void appendParamsClauseAndSetParamsValues(ADBField[] fields, StringBuilder buf,
                                                      List<Object> params, String separator) throws Exception {
        for (ADBField fld: fields) {
            if (fld.sqlCommand != null) {
                Map<String, String> substs = new HashMap<String, String>();
                substs.put("field", fld.lFieldName);
                StrSubstitutor subst = new StrSubstitutor(substs);
                buf.append(subst.replace(fld.sqlCommand)).append(separator);
                if (fld.sqlCommandParams != null) {
                    for (Object p: fld.sqlCommandParams) {
                        if (p instanceof DBFieldValue) {
                            params.add(((DBFieldValue) p).asDBValue());
                        } else if (p instanceof DBFieldValue.IEnumDBFieldValue) {
                            params.add(((DBFieldValue.IEnumDBFieldValue) p).toDBValue().asDBValue());
                        } else {
                            params.add(p);
                        }
                    }
                }
            } else {
                buf.append(fld.lFieldName).append(" = ? ").append(separator);
                params.add(fld.getValue().asDBValue());
            }
        }
        // Удаляем последний разделитель
        buf.setLength(buf.length() - separator.length());
    }

    public static void logParams(ADBField[] fields) {
        if (!LOG.isDebugEnabled())
            return;
        for (ADBField f: fields) {
            DBFieldValue val = f.getValue();
            if (val != null &&
                    val.typeId != FieldTypeId.tid_BLOB &&
                    val.typeId != FieldTypeId.tid_MEMO)
                LOG.debug("params: {}", f.toString());
        }
    }

    public static DMLCommand buildUpdateCommand(String table, String schema, ADBField[] setFields, ADBField[] whereFields) throws Exception {
        StringBuilder buf = new StringBuilder();
        if (schema !=null && !schema.isEmpty()) {
            table = schema + "." + table;
        }
        buf.append("UPDATE ").append(table).append(" SET ");
        List<Object> params = new ArrayList<Object>(setFields.length+ whereFields.length);
        logParams(setFields);
        appendParamsClauseAndSetParamsValues(setFields, buf, params, ", ");
        if (whereFields.length != 0) {
            buf.append("WHERE ");
            appendParamsClauseAndSetParamsValues(whereFields, buf, params, " AND ");
        }
        return new DMLCommand(buf.toString(), params.toArray());
    }

}
