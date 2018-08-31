package ru.sonarplus.kernel.request;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import ru.sonarplus.kernel.sqlobject.common_utils.ValuesSupport;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class RequestData {

    private String request = null;
    private Attributes attributes = null;

    private RequestData() { super(); }

    public static class Attributes {
        private boolean useNulls = false;
        private LocalDateTime workStationLocalDateTime = null;

        private Attributes() { super(); }
        public boolean useNulls() { return this.useNulls; }
        public LocalDateTime workStationLocalDateTime() { return this.workStationLocalDateTime; }

        private static final Map<String, Attributes> mapAttributes = new HashMap<>();
        private static final Attributes defaultAttributes = new Attributes();

        public static Attributes parse(String jsonStrValue)
                throws ParseException {
            // {"use_nulls": true[, "workstation_datetime": "dd.MM.yyyy HH:mm:ss"]}
            Attributes result = mapAttributes.get(jsonStrValue);
            if (result == null) {
                if (StringUtils.isEmpty(jsonStrValue))
                    result = defaultAttributes;
                else
                    result = internalParse((JSONObject)new JSONParser().parse(jsonStrValue));
                mapAttributes.put(jsonStrValue, result);
            }
            return result;
        }

        protected static Attributes internalParse(JSONObject value) {
            Attributes result = new Attributes();
            if (value == null)
                return defaultAttributes;

            Boolean useNulls = (Boolean) value.get("use_nulls");
            result.useNulls = useNulls != null && useNulls;

            String strWSDateTime = (String) value.get("workstation_datetime");
            if (!StringUtils.isEmpty(strWSDateTime))
                result.workStationLocalDateTime = ValuesSupport.parseDateTime(strWSDateTime);
            return result;
        }
    }

    public String getRequest() { return this.request; }

    public Attributes getAttributes(){ return attributes; }

    public static RequestData parse(String value)
            throws ParseException{
        Preconditions.checkArgument(!StringUtils.isEmpty(value));
        // постараемся "угадать", что сюда пришло:
        // - просто текст запроса sql;
        // - просто текст запроса json-sql;
        // - текст запроса (sql|json-sql [и атрибуты запроса (использование null-ов, локальная дата с рабочей станции)])
        if (value.charAt(0) == '{') {
            // предположим, что это json-строка вида:
            // {"request": "..."[, "attributes":{}]}
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonRequestInfo = (JSONObject)jsonParser.parse(value);

            String request = (String)jsonRequestInfo.get("request");
            JSONObject jsonAttributes = (JSONObject)jsonRequestInfo.get("attributes");

            if (request == null && jsonAttributes == null) {
                // в переданной json-строке нет полей ни "request", ни "attributes".
                // возможно, что передали непосредственно запрос json-sql
                RequestData result = new RequestData();
                result.request = value;
                result.attributes = Attributes.defaultAttributes;
                return result;
            }
            else {
                // в противном случае должна быть хотя бы строка запроса
                Preconditions.checkState(!StringUtils.isEmpty(request));

                Attributes attributes = Attributes.parse(jsonAttributes == null ? null : jsonAttributes.toString());

                RequestData result = new RequestData();
                result.request = request;
                result.attributes = attributes;
                return result;
            }
        }
        else {
            // возможно просто запрос sql
            RequestData result = new RequestData();
            result.request = value;
            result.attributes = Attributes.defaultAttributes;
            return result;
        }
    }
}
