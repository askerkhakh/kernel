package ru.sonarplus.kernel.sqlobject.objects;

/* Выражение для представления результатов ранжирования полнотекстового поиска.
    Используется в разделе сортировок, на сервер приложений отправляется как обычное sql-выражение.
    Этот объект не подлежит отправке на сервер приложений и должен обрабатываться при дистиляции.
    В случае Oracle представляет собой вызов функции [l]score(Marker).
    Но нужно обобщить на случай разных СУБД. */

@Deprecated
public class OraFTSRange extends Expression {

    public OraFTSRange() { super(); }

    public OraFTSRange(String expr) {
        this();
        this.expr = expr;
        this.isPureSql = true;
    }

}
