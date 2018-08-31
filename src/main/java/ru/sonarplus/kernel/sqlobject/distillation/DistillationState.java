package ru.sonarplus.kernel.sqlobject.distillation;

import com.google.common.base.Preconditions;
import ru.sonarplus.kernel.dbschema.DbSchemaSpec;
import ru.sonarplus.kernel.dbschema.FieldTypeId;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.objects.*;

import java.util.*;

import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_UNKNOWN;

public class DistillationState {
    // корневой запрос с перечнем параметров - нужен при дистилляции ParamRef-ов
    private SqlQuery root = null;
    // словарь для разрешения имён полей - нужен при дистилляции QualifiedRField-ов
    private Resolver resolver = null;
    private List<String> errors = null;
    // состояния дистилляции, зависящие от контекста дистилляции элемента:
    // ...охватывающий, "родительский", запрос. меняется каждый раз, при начале дистилляции запроса (подзапроса).
    // по завершении - востанавливается.
    // замечание: при дистилляции Select'а, подчинённого Cursor'у parent'ом останется Cursor.
    public SqlQuery contextParent = null;
    // ...требуемый тип значения. в контексте сравнений - дистиллировали первый операнд (поле),
    // определив после дистилляции его тип - требуем от второго операнда такой-же тип (приводим значение к нужному типу).
    private FieldTypeId contextNeededType = tid_UNKNOWN;
    // ...флаг "выражение преобразовано в дерево". при дистилляции строки выражения происходит
    // извлечение, по-возможности, из строки отдельных элементов, замена их ссылками вида '??' и подчинение их как отдельных
    // sql-объектов исходному выражению.
    // после построения дерева для данного выражения, чтобы избежать повторных пробежек при дистилляции подчинённых объектов - взводим флаг
    private boolean contextExpressionTreeBuilt = false;
    // ...текущий CTE-запрос. т.к. Ansi не позволяет вложенные друг в друга CTE - при наличии в sqlobject-запросе CTE-запросов
    private CommonTableExpression contextCurrentCTE = null;
    // будем отслеживать вложенность и, при необходимости, вложенные CTE будем "поднимать наверх", размещая перед охватывающим.
    private int contextCurrentCTEIndex = -1;
    // ...текущие дистиллируемые CTE-запросы
    // т.к. каждый CTE-запрос может обращаться к другим CTE, в т.ч. и рекурсивно к себе - будем избегать повторных дистилляций
    public Set<CommonTableExpression> contextDistillatingCTEs = new HashSet<>();
    /* текущая вершина цепочки union-ов.
         при дистилляции Select-запроса, являющегося вершиной unions-цепочки запоминается ссылка на него.
         все последующие в цепочке запросы Select сопоставляются с вершиной цепочки, в словаре cacheSelectToHeadOfUnionsChainMap.
         требуется для определения охватывающего пространства имён, к которому следует отнести тот или иной запрос Select
         к примеру, есть запрос вида
         select 1 ... where (p in (select 11 union select 12 union select 13))
         union
         select 2 ...
         union
         select 3
         запрос select 1 подчинён "корневому" пространству имён,
         соответственно select 2/3, как элементы union-цепочки, будут отнесены тоже к корню.
         запросы select 11/12/13 будут подчинены пространству имён запроса select 1, который, в свою очередь,
         содержится в "корне"
    */
    public Select contextHeadOfUnionsChain = null;

    // кеши дистилляции:
    // ...незадействованные CTE-запросы
    // при начале дистилляции SELECT'а имеющего непустой раздел WITH, все, имеющиеся
    // в нём CTE-запросы помещаем в этот массив. по-мере дистилляции раздела FROM CTE-запросы,
    // к которым будет происходить обращение, будут исключаться из списка.
    // по завершении дистилляции все, незадействованные CTE будут из запроса удалены.
    public Set<CommonTableExpression> cacheCTEsNotUsedYet = new HashSet<>();
    // ...параметры,
    // при дистилляции ссылки на параметр в контексте условия сравнения - его значение может быть приведено
    // к типу другого операнда. Когда это произойдет первый раз - параметр будет включён в список.
    // далее, при последующих дистилляциях ссылок на этот-же параметр будет контроль на предмет
    // соответствия очередного требуемого типа зафиксированному.
    // TODO #BAD# это вполне может привести к ситуации, когда один и тот же параметр будет участвовать в сравнениях
    // одновременно с полями LONG и INTEGER. Эта ситуация будет ошибочной. Но может быть это и не так важно.
    public Map<String, FieldTypeId> cacheParamsFixedValueTypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    // ...незадействованные параметры
    // при начале дистилляции корневого запроса все параметры включаются в массив. по-мере дистилляции
    // параметры будут из него исключаться. после дистилляции все оставшиеся в массиве параметры будут удален из запроса
    public Set<QueryParam> cacheParamsNotUsedYet = new HashSet<>();
    // ...sql-выражения и количество ссылок в них
    // при дистилляции выражений, подвергнутых перобразованию в деревья,
    // проверяется соответствие количества имеющихся в выражении ссылок '??' и подчинённых объектов.
    // т.к. одно и то-же выражение (с разными аргументами) может встречаться в запросе не один раз -
    // чтобы каждый раз не считать, пробегая по строке - закешируем
    public Map<String, Integer> cacheExprRefCounts = new HashMap<>();
    // ...сопоставление Select-запроса и вершины union-цепочки, содержащей данный select
    public Map<Select, Select> cacheSelectToHeadOfUnionsChainMap = new HashMap<>();

    // счётчик встреченных в запросе маркеров полнотекстовых условий
    public int oraFTSMarkersCount = 0;

    DistillationState(){}

    DistillationState setNeededType(FieldTypeId value) {
        this.contextNeededType = value;
        return this;
    }

    public FieldTypeId getNeededType () {
        return contextNeededType;
    }

    public SqlQuery getRoot()
            throws DistillationException {
        if (this.root == null)
            throw new DistillationException("Не определён корневой запрос");
        return root;
    }

    public void setRoot(SqlQuery item) {
        this.root = item;
    }

    public void setExpressionTreeBuilt(boolean value) {
        Preconditions.checkState(value != this.contextExpressionTreeBuilt);
        this.contextExpressionTreeBuilt = value;
    }

    public boolean isExpressionTreeBuilt() { return this.contextExpressionTreeBuilt; }

    public void clear() {
        if (this.errors != null)
            this.errors.clear();
        this.cacheCTEsNotUsedYet.clear();
        this.contextDistillatingCTEs.clear();
        this.cacheParamsFixedValueTypes.clear();
        if (this.resolver != null)
            this.resolver.clear();
        this.root = null;
        this.contextCurrentCTE = null;
        this.cacheParamsNotUsedYet = null;
        this.contextParent = null;
        this.contextHeadOfUnionsChain = null;
        this.cacheExprRefCounts.clear();
        this.cacheSelectToHeadOfUnionsChainMap.clear();
    }

    public CommonTableExpression getCurrentCTE() {
        return this.contextCurrentCTE;
    }

    public void setCurrentCTE(CommonTableExpression value) {
        Preconditions.checkState((this.contextCurrentCTE == null && value != null) || value == null);
        this.contextCurrentCTE = value;
        if (this.contextCurrentCTE != null) {
            CTEsContainer owner = (CTEsContainer) this.contextCurrentCTE.getOwner();
            Preconditions.checkNotNull(owner);
            this.contextCurrentCTEIndex = owner.indexOf(this.contextCurrentCTE);
        }
    }

    public void setEarlyThanCurrentCTE(CommonTableExpression cte)
            throws SqlObjectException {
        Preconditions.checkNotNull(cte);
        Preconditions.checkState(this.contextCurrentCTE != cte);
        ((CTEsContainer)(this.contextCurrentCTE.getOwner())).insertItem(cte, this.contextCurrentCTEIndex);
    }

    public void throwBulkErrors ()
            throws DistillationException {
        if (this.errors != null)
            throw new DistillationException("\n" + String.join("\n ", this.errors));

    }

    public void throwError(String msg, SqlObject item)
            throws DistillationException {
        String s;
        if (item == null)
            s = msg;
        else {
            if (item.getOwner() == null)
                s = item.getClass().getName();
            else
                s = String.format("%s (содержащийся в %s)", item.getClass().getName(), item.getOwner().getClass().getName());
            s = s + ": " + msg;
        }
        // бросаем исключение сразу...
        throw new DistillationException(s);
        // ...или накапливаем информацию об ошибках дистилляции
/*
        if (this.errors == null)
            this.errors = new ArrayList<>();
        this.errors.add(s);
*/
    }

    private boolean needResolver = true;

    public DistillationState needResolver(boolean value) {
        this.needResolver = value;
        return this;
    }

    private DbSchemaSpec schema = null;

    public DistillationState setSchema(DbSchemaSpec value) {
        this.schema = value;
        return this;
    }

    DistillationState setResolver(Resolver value) {
        // вообще говоря, для ускорения разрешения имён полей в каких-то запросах, теоретически можно было бы подключать
        // простые словари, содержащие пары <имя>-<techInfo>, работающие без учёта пространств имён...
        this.resolver = value;
        return this;
    }

    public Resolver getResolver ()
            throws DistillationException {
        if (this.resolver == null) {
            if (this.needResolver) {
                if (this.schema == null)
                    throw new DistillationException("Для словаря имён требуется схема");
                this.resolver = new Resolver(this.schema);
            }
            else
              throw new DistillationException("Не задан словарь для разрешения имён");
        }
        return resolver;
    }

    protected void addResolverFor(DataChangeSqlQuery parent, Map<Select, Select> cacheTopSelectOfUnionsForSelect)
            throws DistillationException {
        if (this.needResolver)
            getResolver().addResolverFor(parent, cacheTopSelectOfUnionsForSelect);
    }

    protected void addResolverFor(String sourceTable, String sourceName, SqlQuery parent, Map<Select, Select> cacheTopSelectOfUnionsForSelect)
            throws DistillationException {
        if (this.needResolver)
            getResolver().addResolverFor(sourceTable, sourceName, parent, cacheTopSelectOfUnionsForSelect);
    }

    protected void addResolverFor(CommonTableExpression sourceDistillatedCTE, String sourceName, SqlQuery parent, Map<Select, Select> cacheTopSelectOfUnionsForSelect)
            throws DistillationException {
        if (this.needResolver)
            getResolver().addResolverFor(sourceDistillatedCTE, sourceName, parent, cacheTopSelectOfUnionsForSelect);
    }

    protected void addResolverFor(Select sourceDistillatedSelect, String sourceName, SqlQuery parent, Map<Select, Select> cacheTopSelectOfUnionsForSelect)
            throws DistillationException {
        if (this.needResolver)
            getResolver().addResolverFor(sourceDistillatedSelect, sourceName, parent, cacheTopSelectOfUnionsForSelect);
    }

}
