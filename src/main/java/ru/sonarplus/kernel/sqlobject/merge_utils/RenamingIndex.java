package ru.sonarplus.kernel.sqlobject.merge_utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.RenamingDict;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.*;

/**
 * Класс поддержки переименования элементов объединяемых запросов, "индекс":
 * - параметров
 * - CTE-запросов
 * - алиасов таблиц
 * - алиасных частей в квалифицированных именах
 *
 */
public class RenamingIndex {

    private static final String NO_RENAMING_PARAM_PREFIX = "NRP_" + "6XDARN5LS" + "_";
    private static final String STANDART_PARAMETER_NAME_PREFIX = "P_$_";
    private static final String CTE_PREFIX = "CTE_";
    private static final String JOINED_SUFFIX = "_J";

    private JoiningPathBuilder joiningPathBuilder = new JoiningPathBuilder();
    private Map<Select, RenamingDict> paramsRenaming = new HashMap<>();
    private Set<String> paramNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private int paramsCount = 0;

    private Map<Select, RenamingDict> ctesRenaming = new HashMap<>();
    private Set<CommonTableExpression> listOfCTEs = new HashSet<>();
    private Map<Select, RenamingDict> tablesRenaming = new HashMap<>();
    private Set<String> tableNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private int tablesCount = 0;

    private Map<FromClauseItem, String> joiningPaths = new HashMap<>();
    private Map<String, JoiningPathInfo> joiningPathInfos = new HashMap();

    public RenamingIndex() {

    }

    /**
     * Построение для перечня запросов "индекса", далее импользуемого при объединении
     * @param selects
     *   перечень запросов
     * @return
     * @throws InvalidClassOfChild
     */
    public RenamingIndex build(Select... selects)
            throws SqlObjectException {
        if (selects.length == 1)
            return this;

        for(Select select: selects)
            buildRenamingIndexForSelect(select);
        freeFromItems();
        return this;
    }

    public RenamingDict getParamsRenamingFor(Select select) {
        return getDictFor(select, paramsRenaming);
    }

    public RenamingDict getCTEsRenamingFor(Select select) {
        return getDictFor(select, ctesRenaming);
    }

    public RenamingDict getTablesRenamingFor(Select select) {
        return getDictFor(select, tablesRenaming);
    }

    /**
     * Проверка дублирования слияния для элемента from переданного запроса
     * @param from
     * @param indexedSelect
     * @return
     * Дублирование определяется по наличию таких-же "путей слияния" для других элементов
     * разделов from в этом-же или других объединяемых запросах
     */
    public boolean isJoiningPathDuplicates(FromClauseItem from, Select indexedSelect) {
        String joiningPath = this.joiningPaths.get(from);
        JoiningPathInfo joiningPathInfo = this.joiningPathInfos.get(joiningPath);
        return joiningPathInfo != null &&
                (
                        joiningPathInfo.select != indexedSelect ||
                        (joiningPathInfo.select == indexedSelect && joiningPathInfo.from != from)
                );
    }

    protected RenamingDict getDictFor(Select select, Map<Select, RenamingDict> dict) {
        if (!dict.containsKey(select)) {
            RenamingDict dictRenaming = new RenamingDict();
            dict.put(select, dictRenaming);
            return dictRenaming;
        }
        else
            return dict.get(select);
    }

    protected void buildRenamingIndexForSelect(Select select)
            throws SqlObjectException {
        CTEsContainer with = select.findWith();
        if (with != null)
            for(SqlObject item: with)
                addCTE(select, (CommonTableExpression) item);

        boolean first = true;
        for(SqlObject item: Preconditions.checkNotNull(select.getFrom())) {
            addFromItem(select, (FromClauseItem) item, !first);
            first = false;
        }
        recursiveWalk(select, select);
    }

    protected void recursiveWalk(SqlObject item, Select select)
            throws SqlObjectException {
        if (item instanceof Parameter)
            addParam(select, (Parameter) item);
        else if (item instanceof FromClauseItem && SqlObjectUtils.getParentSelect(item) != select)
            addFromItem(select, (FromClauseItem) item, true);
        for(SqlObject child: item)
            recursiveWalk(child, select);
    }

    protected String createNewParamName(String orgName) {
        String result = orgName;
        while (paramNames.contains(result))
            result = STANDART_PARAMETER_NAME_PREFIX + Integer.toString(paramsCount++);
        paramNames.add(result);
        return result;
    }

    protected String createNewCTEName(String orgName) {
        String result = orgName;
        while (tableNames.contains(result))
            result = CTE_PREFIX + Integer.toString(tablesCount++);
        tableNames.add(result);
        return result;
    }

    protected String createNewTableAias(String orgName, boolean isJoined, boolean isNeedAlias) {
        String result = orgName;
        if (isNeedAlias) {
            while (tableNames.contains(result))
                result = (isJoined ? orgName + JOINED_SUFFIX : orgName) + Integer.toString(tablesCount++);
            tableNames.add(result);
        }
        return result;
    }

    protected void addParam(Select select, Parameter param) {
        if (param.isContainedInParamsClause())
            // вынесенные в раздел параметров запроса параметры не должны быть "техническими" - должны иметь имена
            Preconditions.checkState(!SqlObjectUtils.isTechParam(param));
        if (SqlObjectUtils.isTechParam(param))
            // "технические" параметры в теле запроса не трогаем
            return;

        if (param.parameterName.startsWith(NO_RENAMING_PARAM_PREFIX))
            // параметры c префиксом "не перименовывать" не переименовываем
            return;

        RenamingDict paramsDict = getParamsRenamingFor(select);
        if (!paramsDict.isRenamed(param.parameterName))
            paramsDict.add(param.parameterName, createNewParamName(param.parameterName));
    }

    protected void addCTE(Select select, CommonTableExpression cte) {
        if (cte == null)
            return;
        if (listOfCTEs.contains(cte))
            return;
        getCTEsRenamingFor(select)
                .add(cte.alias, createNewCTEName(cte.alias));
        listOfCTEs.add(cte);
    }

    protected void addFromItem(Select select, FromClauseItem from, boolean isNeedAlias)
            throws SqlObjectException {
        CommonTableExpression cte = SqlObjectUtils.CTE.findCTE(from);
        if (cte == null)
            addFromItemRenaming(select, from, null, isNeedAlias);
        else {
            addCTE(select, cte);
            addFromItemRenaming(select, from, getCTEsRenamingFor(select), isNeedAlias);
        }
    }

    protected class JoiningPathInfo {
        public Select select;
        public FromClauseItem from;
        public String alias;

        public JoiningPathInfo(Select select, FromClauseItem from, String alias) {
            this.select = select;
            this.from = from;
            this.alias = alias;
        }
    }

    private List<FromClauseItem> fromItemsForFree = null;

    protected void addFromItemForFree(FromClauseItem from) {
        if (fromItemsForFree == null)
            fromItemsForFree = new ArrayList<FromClauseItem>();
        fromItemsForFree.add(from);
    }

    protected void freeFromItems()
            throws SqlObjectException {
        if (fromItemsForFree != null)
            for (FromClauseItem from: fromItemsForFree)
                from.getOwner().removeItem(from);
    }

    protected void addFromItemRenaming(Select select, FromClauseItem from, RenamingDict renamingCTEs, boolean isNeedAlias)
            throws SqlObjectException {
        String aliasOrName = from.getAliasOrName();
        // Если у элемента From нет ни алиаса, ни имени, значит квалифицированных ссылок на этот элемент
        //    нет, значит и переименовывать нечего.
        if (StringUtils.isEmpty(aliasOrName))
            return;
        if (SqlObjectUtils.getParentSelect(from) == select) {
            // элемент From корневого запроса
            String joiningPath = joiningPathBuilder.execute(from, renamingCTEs);
            joiningPaths.put(from, joiningPath);
            // в данном или ранее обработанных запросах такое-же слияние уже встречалось?
            JoiningPathInfo joiningPathInfo = joiningPathInfos.get(joiningPath);
            if (joiningPathInfo != null) {
                // встречалось. переименуем данный элемент таким-же образом
                addTableRenaming(select, aliasOrName, getTablesRenamingFor(joiningPathInfo.select).rename(joiningPathInfo.alias));
                // При объединении двух и более запросов данный элемент в результирующий запрос
                // не будет включён из-за дублирования путей слияния,
                // но если запрос будет только один - будет возвращена его копия.
                // Позаботимся, чтобы дублирующийся подлитый элемент в результат точно не попал. }
                if (!from.getIsFirst())
                    // добавим элемент from  в перечень на удаление
                    // здесь удалить не можем, т.к. иначе возникнет конфликт с итератором, обходящим раздел FROM
                    addFromItemForFree(from);
            }
            else {
                // такой путь слияния встретился первый раз
                joiningPathInfos.put(joiningPath, new JoiningPathInfo(select, from, aliasOrName));
                addTableRenaming(select, aliasOrName, createNewTableAias(aliasOrName, from.getIsJoined(),isNeedAlias));
            }
        }
        else
            // элемент From подзапроса
            // Алиасы таблиц подзапросов просто при необходимости переименуем (ТАБЛИЦА[_J]NN)
            addTableRenaming(select, aliasOrName, createNewTableAias(aliasOrName, from.getIsJoined(), isNeedAlias));
    }

    protected void addTableRenaming(Select select, String source, String target) {
        RenamingDict tablesDict = getTablesRenamingFor(select);
        if (!tablesDict.isRenamed(source)) {
            // не нужно нам переименование в пустое имя,
            //      т.к. это может привести (и приведёт) к тому,
            //  что алиасные части квалифицированных имён полей
            //  будут отброшены и в результате будет неверный запрос
            Preconditions.checkNotNull(target);
            Preconditions.checkState(!target.isEmpty());
            tablesDict.add(source, target);
        }

    }
}
