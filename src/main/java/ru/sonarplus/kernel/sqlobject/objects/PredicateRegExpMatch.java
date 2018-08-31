package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class PredicateRegExpMatch extends PredicateTextSearch {
	public boolean caseSensitive;
	public boolean pointAsCRLF;
	public boolean multiLine;

	public PredicateRegExpMatch() { super(); }

	public PredicateRegExpMatch(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}

}
