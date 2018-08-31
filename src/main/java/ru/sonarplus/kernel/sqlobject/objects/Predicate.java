package ru.sonarplus.kernel.sqlobject.objects;

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class Predicate extends SqlObject {
	public boolean not = false;
	public String hint = null;

	public Predicate() { super(); }

    public Predicate(SqlObject owner) throws SqlObjectException { super(owner); }

    public <T extends Predicate> T setNot(boolean not) {
        this.not = not;
        return (T) this;
    }

    public <T extends Predicate> T setHint(String hint) {
        this.hint = hint;
        return (T) this;
    }
}
