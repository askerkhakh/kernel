package ru.sonarplus.kernel.sqlobject.objects;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class SourceTable extends Source {
	public String schema;
	public String table;

    public SourceTable() { super(); }

	public SourceTable(String table) {
		this();
		setFullTableName(table);
	}

    public SourceTable(SqlObject owner)
            throws SqlObjectException {
        super(owner);
    }

	public SourceTable(SqlObject owner, String table)
            throws SqlObjectException {
		this(owner);
		setFullTableName(table);
	}

	@Override
	public String getTable() {
		return table;
	}

	@Override
	public void setTable(String value) {
		Preconditions.checkNotNull(value);
		table = value.trim();
		Preconditions.checkState(!StringUtils.isEmpty(table),
				"%s: Не задано имя таблицы",
				this.getClass().getSimpleName());
		
	}

	@Override
	public String getSchema() {
		return schema;
	}

	@Override
	public void setSchema(String value) {
		schema = value;
	}

}
