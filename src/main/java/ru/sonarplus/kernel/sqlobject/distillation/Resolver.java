package ru.sonarplus.kernel.sqlobject.distillation;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.dbschema.*;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;
import ru.sonarplus.kernel.sqlobject.common_utils.QualifiedName;
import ru.sonarplus.kernel.sqlobject.objects.*;
import ru.sonarplus.kernel.sqlobject.sqlobject_utils.SqlObjectUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// словарь для разрешения имён полей, с учётом пространств имён запроса.
// представляет собой дерево узлов, соответствующих пространствам имён запроса.
/*
    Ресолвер представляет собой набор узлов (ResolvingNode), сопоставленных с соответствующими запросами (this.nodes).
    Узлы находятся в отношениях подчинённости между собой, что позволяет делегировать разрешение имени
    узлу верхнего уровня, при невозможности сделать это на текущем уровне.

    В конечном итоге все неудачные запросы на разрешение будут переданы корневому узлу (root),
    возвращающему null.

    Узлы строятся по-мере дистилляции запросов и их разделов.
    Пример запроса и дерева узлов и сопоставления их с запросами:

    cursor
      with
        cte1() as (select cte1),
        cte2() as (select cte2)
      select1 from table1
              from table2
              from (select11) sub
      ...
      order by

    дерево epkjd:
    root
      node_select_cte1
      node_select_cte2
      node_select11         // подзапросы в разделах FROM и CTE-запросы относятся к корню
      node_cursor
        node_table1
          node_table2
            node_sub  // узлы добавляются по мере обработки элементов раздела FROM

    сопоставление с узлами:
     select cte1 -> node_select_cte1
     select cte2 -> node_select_cte2
     select11 -> node_select11
     cursor -> node_sub
     select1 -> node_sub

* */
public class Resolver {
    // ссылка на схему - не обязательна, но при наличии в запросе QualifiedRField-ов
    // отсутствие схемы приведёт к невозможности их разрешения
    private DbSchemaSpec schemaSpec;
    // корень дерева. в ответ на любой запрос (resolve(), getRecId(), getAsterisk())вернёт null
    private TerminalNode root;
    // собственно узлы дерева (пространства имён), сопоставленные с основным запросом и подзапросами
    private Map<SqlObject, ResolvingNode> nodes = new HashMap<>();

    Resolver(DbSchemaSpec schemaSpec) {
        this.schemaSpec = schemaSpec;
        root = new TerminalNode(null);
    }

    public ColumnExprTechInfo resolve(QualifiedName qname, SqlQuery parent, Map<Select, Select> cacheTopSelectOfUnionsForSelect) {
        Preconditions.checkNotNull(qname);
        Preconditions.checkArgument(!StringUtils.isEmpty(qname.name));
        return getResolvingNodeFor(parent, cacheTopSelectOfUnionsForSelect).resolve(qname);
    }

    public ColumnExprTechInfo[] getRecId(String tableName, SqlQuery parent, Map<Select, Select> cacheTopSelectOfUnionsForSelect) {
        Preconditions.checkArgument(!StringUtils.isEmpty(tableName));
        return getResolvingNodeFor(parent, cacheTopSelectOfUnionsForSelect).getRecId(tableName);
    }

    public ColumnExprTechInfo[] getAsteriskFields(String tableName, SqlQuery parent, Map<Select, Select> cacheTopSelectOfUnionsForSelect) {
        return getResolvingNodeFor(parent, cacheTopSelectOfUnionsForSelect).getAsteriskFields(tableName);
    }

    public void clear() {
        nodes.clear();
    }

    protected ResolvingNode getResolvingNodeFor(SqlQuery parent, Map<Select, Select> cacheSelectToHeadOfUnionsChainMap)
            throws DistillationException {
        // попробуем получить охватывающее пространство имён для parent'а
        if (parent == null)
            // пока такого быть не должно - для получения охватывающего запроса
            // сейчас используется метод SqlObjectUtils.getParentQuery(), который при отсутствии охватывающего запроса
            // выбрасывает исключение.
            // тем не менее, если такое будет позволено (мало ли - захотим строить какой-то специальный, быстрый ресолвер для специфичных запросов )
            // - отнесём элемент к корню пространства имён.
            return root;

        // попытались найти сопоставленный запросу узел...
        ResolvingNode node = nodes.get(parent);
        if (node != null)
            return node;

        if (parent.getClass() != Select.class) {
            // люой запрос, который не может быть подзапросом: Cursor.../DML/CallSP... - в корневом пространстве имён
            nodes.put(parent, root);
            return root;
        }

        // - select может содержаться в цепочке union-ов;
        // - соответственно нужно пространство имён, охватывающее данную цепочку;
        // - для этого в цепочке union-ов нам нужен самый верхний select
        Select headOfUnionsChain = cacheSelectToHeadOfUnionsChainMap.get(parent);

        if (headOfUnionsChain == null)
            // для данного select-а уже должен быть известен самый верхний select в цепочке union-ов -
            // запоминается при начале дистилляции Select'а
            throw new DistillationException("Не удалось получить вершину union-цепочки");


        SqlObject unionsChainOwner = headOfUnionsChain.getOwner();

        if (unionsChainOwner == null) {
            // если headOfUnionsChain цепочки union-ов является корнем - текущий select сопоставляем к корневому пространству имён
            nodes.put(parent, root);
            return root;
        }

        Class topSelectOfUnionsOwnerClass = unionsChainOwner.getClass();

        if (topSelectOfUnionsOwnerClass == CursorSpecification.class) {
            // если headOfUnionsChain цепочки union-ов содержится в CursorSpec'е - текущий select относим к пространству
            // имён, содержащему CursorSpec - т.е. к корневому.
            node = getResolvingNodeFor((CursorSpecification) headOfUnionsChain.getOwner(), null/*cacheSelectToHeadOfUnionsChainMap*/);
            nodes.put(parent, node);
            return node;
        }

        if (topSelectOfUnionsOwnerClass == Scalar.class ||
                topSelectOfUnionsOwnerClass == PredicateExists.class ||
                topSelectOfUnionsOwnerClass == PredicateInQuery.class) {
            // подзапрос в теле охватывающего запроса - не в блоке FROM - включаем в пространство имён, содержащее охватывающий запрос
            node = getResolvingNodeFor(SqlObjectUtils.getParentQuery(unionsChainOwner), cacheSelectToHeadOfUnionsChainMap);
            nodes.put(parent, node);
            return node;
        }

        if (headOfUnionsChain.getOwner().getClass() == SourceQuery.class || topSelectOfUnionsOwnerClass == CommonTableExpression.class) {
            // подзапрос в разделе From или cte-запрос  - в своём, т.е. в корневом пространстве имён
            nodes.put(parent, root);
            return root;
        }

        // вроде всё, что нужно рассмотрели, но если вдруг сюда добрались - значит что-то пошло не так.
        throw new DistillationException("При разрешении пространств имён дистиллируемого запроса что-то пошло не так...");
    }

    protected abstract class ResolvingNode {

        protected ResolvingNode owner;

        ResolvingNode(ResolvingNode owner) {
            this.owner = owner;
        }

        public ColumnExprTechInfo resolve(QualifiedName qname) { return null; }
        public ColumnExprTechInfo[] getRecId(String tableName) { return null; }
        public ColumnExprTechInfo[] getAsteriskFields(String tableName) { return null; }
    }

    protected class TerminalNode extends ResolvingNode {

        TerminalNode(ResolvingNode owner) { super(owner); }

    }

    protected abstract class TableExprNode extends ResolvingNode {
        private Map<String, ColumnExprTechInfo> map = null;
        protected String name;
        protected ColumnExprTechInfo[] cachedAsteriskFields;


        TableExprNode(ResolvingNode owner) {
            super(owner);
        }

        public TableExprNode setSourceName(String value) {
            Preconditions.checkArgument(!StringUtils.isEmpty(value));
            this.name = value;
            return this;
        }

        @Override
        public ColumnExprTechInfo resolve(QualifiedName qname) {
            String sqname = qname.qualifiedNameToString();
            ColumnExprTechInfo result = getFromMap(sqname);
            if (result != null)
                return result.getClone();

            result = internalResolve(qname);
            if (result == null && owner != null)
                result = owner.resolve(qname);
            if (result != null)
                addToMap(sqname, result);

            return result != null ? result.getClone() : null;
        }

        @Override
        public ColumnExprTechInfo[] getRecId(String tableName) {
            ColumnExprTechInfo[] result = internalGetRecId(tableName);
            if (result == null && owner != null)
                result = owner.getRecId(tableName);
            return result;
        }

        @Override
        public ColumnExprTechInfo[] getAsteriskFields(String tableName) {
            if (StringUtils.isEmpty(tableName)) {
                // возьмём перечень полей из ранее определённых таблиц, затем добавим свои
                ColumnExprTechInfo[] topFields = owner.getAsteriskFields(tableName);
                ColumnExprTechInfo[] mineFields = this.internalGetCachedAsteriskFields();
                ColumnExprTechInfo[] result = null;
                if (topFields != null) {
                    result = new ColumnExprTechInfo[topFields.length + (mineFields != null ? mineFields.length : 0)];
                    if (result.length == 0)
                        return null;
                    int i = 0;
                    for (ColumnExprTechInfo techInfo: topFields)
                        result[i++] = techInfo;
                    if (mineFields != null)
                        for (ColumnExprTechInfo techInfo: mineFields)
                            result[i++] = techInfo;
                    return result;
                }
                else if (mineFields != null && mineFields.length != 0)
                    return mineFields;
                return null;
            }
            else if (tableName.equalsIgnoreCase(name))
                // вернём свой перечень полей
                return this.internalGetCachedAsteriskFields();
            else if (owner != null)
                return owner.getAsteriskFields(tableName);
            else
                return null;
        }

        protected ColumnExprTechInfo getFromMap(String qname) {
            if (map != null)
                return map.get(qname);
            return null;
        }

        protected void addToMap(String qname, ColumnExprTechInfo fieldSpec) {
            if (map == null)
                map = new HashMap<>();
            map.put(qname, fieldSpec);
        }

        protected ColumnExprTechInfo[] internalGetCachedAsteriskFields() {
            if (cachedAsteriskFields == null) {
                cachedAsteriskFields = buildAsteriskFieldsCache();
                for (ColumnExprTechInfo techInfo: cachedAsteriskFields)
                    techInfo.tableExprName = this.name;
            }
            ColumnExprTechInfo[] result = new ColumnExprTechInfo[cachedAsteriskFields.length];
            for (int i = 0; i < cachedAsteriskFields.length; i++)
                result[i] = cachedAsteriskFields[i].getClone();
            return result;
        }

        protected ColumnExprTechInfo internalResolve(QualifiedName qname) {
            Preconditions.checkNotNull(qname);
            Preconditions.checkArgument(!StringUtils.isEmpty(qname.alias));
            Preconditions.checkArgument(!StringUtils.isEmpty(qname.name));
            if (qname.alias.equals(this.name))
                return internalResolveEx(qname);
            return null;
        }

        protected ColumnExprTechInfo[] internalGetRecId(String tableName) { return null; }

        protected abstract ColumnExprTechInfo internalResolveEx(QualifiedName qname);

        protected abstract ColumnExprTechInfo[] buildAsteriskFieldsCache();
    }

    // таблица
    protected class SourceTableNode extends TableExprNode {

        private DbSchemaSpec schemaSpec = null;
        private String tableName = null;
        private TableSpec tableSpec = null;
        private ColumnExprTechInfo[] recid;

        SourceTableNode(ResolvingNode owner) {
            super(owner);
        }

        SourceTableNode setTableNameAndSchema (String tableName, DbSchemaSpec schemaSpec) {
            /* если в запросе не будет QRField-ов - описатель таблицы не понадобится,
               поэтому пока храним только имя и схему
             */
            this.tableName = tableName;
            this.schemaSpec = schemaSpec;
            return this;
        }

        protected TableSpec getTableSpec() {
            if (this.tableSpec != null)
                return this.tableSpec;
            if (StringUtils.isEmpty(tableName))
                return this.tableSpec;
            else {
                // встретили первый QRField (или RecId или нашу '*'), попробуем найти описатель таблицы
                if (schemaSpec != null)
                  this.tableSpec = schemaSpec.findTableSpec(tableName);
                // описатель таблицы мог быть не найден. чтобы его повторно не искать по схеме - сбросим имя
                this.tableName = null;
                return this.tableSpec;
            }
        }

        @Override
        protected ColumnExprTechInfo[] buildAsteriskFieldsCache() {
            TableSpec tableSpec = this.getTableSpec();
            if (tableSpec == null)
                return null;
            List<ColumnExprTechInfo> listFields = new ArrayList<>();
            for (int i = 0; i < tableSpec.getFieldSpecCount(); i++) {
                FieldSpec fieldSpec = tableSpec.getFieldSpec(i);
                //if (!(fieldSpec.getDataTypeSpec().getFieldTypeId() == FieldTypeId.tid_BLOB || fieldSpec.getDataTypeSpec().getFieldTypeId() == FieldTypeId.tid_MEMO))
                    listFields.add(ColumnExprTechInfo.createTechInfoByFieldSpec(fieldSpec));
            }
            return listFields.toArray(new ColumnExprTechInfo[0]);
        }

        @Override
        protected ColumnExprTechInfo internalResolveEx(QualifiedName qname) {
            TableSpec tableSpec = this.getTableSpec();
            if (tableSpec != null) {
                FieldSpec fieldSpec = tableSpec.findFieldSpecByName(qname.name);
                if (fieldSpec != null) {
                    ColumnExprTechInfo techInfo = ColumnExprTechInfo.createTechInfoByFieldSpec(fieldSpec);
                    techInfo.tableExprName = this.name;
                    return techInfo;
                }
            }
            return null;
        }

        @Override
        protected ColumnExprTechInfo[] internalGetRecId(String tableName) {
            if (tableName.equals(this.name))
                return getCopyOfRecId();
            return null;
        }

        protected ColumnExprTechInfo[] getCopyOfRecId() {
            if (recid == null) {
                FieldSpec[] fieldSpecs = getIFieldSpecs();
                if (fieldSpecs != null && fieldSpecs.length != 0) {
                    recid = new ColumnExprTechInfo[fieldSpecs.length];
                    for (int i = 0; i < fieldSpecs.length; i++) {
                        recid[i] = ColumnExprTechInfo.createTechInfoByFieldSpec(fieldSpecs[i]);
                        recid[i].tableExprName = this.name;
                    }
                }
            }
            if (recid == null)
                return null;
            ColumnExprTechInfo[] result = new ColumnExprTechInfo[recid.length];
            for(int i = 0; i < recid.length; i++)
                result[i] = recid[i].getClone();
            return result;
        }

        public FieldSpec[] getIFieldSpecs() {
            TableSpec tableSpec = this.getTableSpec();
            if (tableSpec == null)
                return null;
            PrimaryKeyConstraintSpec primarySpec = tableSpec.getPrimaryKey();
            FieldSpec[] result;
            if (primarySpec != null) {
                result = new FieldSpec[primarySpec.getItemsCount()];
                for (int i = 0; i < result.length; i++) {
                    result[i] = primarySpec.getItem(i);
                }
                return result;
            }
            for (IndexSpec indexSpec: tableSpec.indexItems) {
                if ((indexSpec.indexType == IndexType.UNIQUE) && (indexSpec.getItemsCount() == 1)) {
                    FieldSpec uniqueSpec = indexSpec.getItem(0).fieldSpec;
                    if (uniqueSpec != null) {
                        return new FieldSpec[]{uniqueSpec};
                    }
                }
            }
            return null;
        }
    }

    // ссылка на cte-запрос
    protected class SourceCTENode extends TableExprNode {
        private CommonTableExpression distillatedCTE;

        SourceCTENode(ResolvingNode owner) {
            super(owner);
        }

        SourceCTENode setDistillatedCTE(CommonTableExpression value) {
            Preconditions.checkNotNull(value);
            this.distillatedCTE = value;
            return this;
        }

        @Override
        protected ColumnExprTechInfo[] buildAsteriskFieldsCache()
                throws SqlObjectException {
            List<ColumnExprTechInfo> listFields = new ArrayList<>();
            SelectedColumnsContainer columns = distillatedCTE.getSelect().getColumns();
            for (int i = 0; i < columns.itemsCount(); i++) {
                ColumnExprTechInfo techInfo = Preconditions.checkNotNull(columns.getColumn(i).getColExpr().distTechInfo);
                techInfo.resetIndexInfo();
                listFields.add(techInfo);
            }
            return listFields.toArray(new ColumnExprTechInfo[0]);
        }

        @Override
        protected ColumnExprTechInfo internalResolveEx(QualifiedName qname) {
            int columnIndex = distillatedCTE.columns.indexOf(qname.name);
            if (columnIndex >= 0) {
                SelectedColumn column = distillatedCTE.getSelect().getColumns().getColumn(columnIndex);
                ColumnExprTechInfo techInfo = Preconditions.checkNotNull(column.getColExpr().distTechInfo).getClone();
                techInfo.dbdFieldName = qname.name;
                techInfo.nativeFieldName = techInfo.dbdFieldName;
                techInfo.resetIndexInfo();
                return techInfo;
            }
            return null;
        }
    }

    // подзапрос в разделе FROM
    protected class SourceSelectNode extends TableExprNode {

        private Select distillatedSelect;

        SourceSelectNode(ResolvingNode owner) {
            super(owner);
        }

        SourceSelectNode setDistillatedSelect(Select value) {
            Preconditions.checkNotNull(value);
            this.distillatedSelect = value;
            return this;
        }

        @Override
        protected ColumnExprTechInfo[] buildAsteriskFieldsCache() {
            List<ColumnExprTechInfo> listFields = new ArrayList<>();
            SelectedColumnsContainer columns = distillatedSelect.getColumns();
            for (int i = 0; i < columns.itemsCount(); i++) {
                SelectedColumn column = columns.getColumn(i);
                ColumnExprTechInfo techInfo = Preconditions.checkNotNull(column.getColExpr().distTechInfo).getClone();
                if (!StringUtils.isEmpty(column.alias)) {
                    techInfo.nativeFieldName = column.alias;
                    techInfo.dbdFieldName = column.alias;
                }
                techInfo.resetIndexInfo();
                listFields.add(techInfo);
            }
            return listFields.toArray(new ColumnExprTechInfo[0]);
        }

        @Override
        protected ColumnExprTechInfo internalResolveEx(QualifiedName qname) {
            SelectedColumnsContainer columns = distillatedSelect.getColumns();
            SelectedColumn column = columns.findColumnByAlias(qname.name);

            if (column != null) {
                // нашли колонку по алиасу
                ColumnExpression colExpr = Preconditions.checkNotNull(column.getColExpr());
                ColumnExprTechInfo techInfo = Preconditions.checkNotNull(colExpr.distTechInfo).getClone();
                techInfo.dbdFieldName = column.alias;
                techInfo.nativeFieldName = column.alias;
                techInfo.resetIndexInfo();
                return techInfo;
            }
            else
                for (SqlObject item : columns) {
                    // ищем колонку без алиаса, содержащую поле, по имени поля
                    column = (SelectedColumn) item;
                    if (!StringUtils.isEmpty(column.alias))
                        continue;
                    ColumnExpression expr = column.getColExpr();
                    if (expr.getClass() == QualifiedField.class && expr.distTechInfo != null) { // всё уже должно быть дистиллировано
                        ColumnExprTechInfo techInfo = expr.distTechInfo;
                        if (techInfo.dbdFieldName != null && techInfo.dbdFieldName.equals(qname.name)) {
                            techInfo = techInfo.getClone();
                            techInfo.resetIndexInfo();
                            return techInfo;
                        }
                    }
                }
            return null;
        }
    }

    protected void addResolverFor(DataChangeSqlQuery parent, Map<Select, Select> cacheTopSelectOfUnionsForSelect)
            throws DistillationException {
        String tableName = SqlObjectUtils.getRequestTableName(parent);
        nodes.put(
                parent,
                new SourceTableNode(getResolvingNodeFor(parent, cacheTopSelectOfUnionsForSelect))
                        .setTableNameAndSchema(tableName, schemaSpec)
                        .setSourceName(tableName)
        );
    }

    protected void addResolverFor(String sourceTable, String sourceName, SqlQuery parent, Map<Select, Select> cacheTopSelectOfUnionsForSelect)
            throws DistillationException {
        nodes.put(
                parent,
                new SourceTableNode(getResolvingNodeFor(parent, cacheTopSelectOfUnionsForSelect))
                        .setTableNameAndSchema(sourceTable, this.schemaSpec)
                        .setSourceName(sourceName)
        );
    }

    protected void addResolverFor(CommonTableExpression sourceDistillatedCTE, String sourceName, SqlQuery parent, Map<Select, Select> cacheTopSelectOfUnionsForSelect)
            throws DistillationException {
        nodes.put(
                parent,
                new SourceCTENode(getResolvingNodeFor(parent, cacheTopSelectOfUnionsForSelect))
                        .setDistillatedCTE(sourceDistillatedCTE)
                        .setSourceName(sourceName)
        );
    }

    protected void addResolverFor(Select sourceDistillatedSelect, String sourceName, SqlQuery parent, Map<Select, Select> cacheTopSelectOfUnionsForSelect)
            throws DistillationException {
        nodes.put(
                parent,
                new SourceSelectNode(getResolvingNodeFor(parent, cacheTopSelectOfUnionsForSelect))
                        .setDistillatedSelect(sourceDistillatedSelect)
                        .setSourceName(sourceName)
        );
    }

}

