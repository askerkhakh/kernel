package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class PredicateFullTextSearch extends PredicateTextSearch {
	public boolean sortByResult;

	public PredicateFullTextSearch() { super(); }

	public PredicateFullTextSearch(ColumnExpression expr, ColumnExpression template) { super(); }

	public PredicateFullTextSearch(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}

}
