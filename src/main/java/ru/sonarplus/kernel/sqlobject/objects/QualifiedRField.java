package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class QualifiedRField extends QualifiedField {

	public QualifiedRField(String alias, String name) {
		super(alias, name);
	}

	public QualifiedRField(SqlObject owner, String alias, String name)
			throws SqlObjectException {
		super(owner, alias, name);
	}

}
