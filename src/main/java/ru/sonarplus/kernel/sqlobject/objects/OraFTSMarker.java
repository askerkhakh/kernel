package ru.sonarplus.kernel.sqlobject.objects;

/*OraFTSMarker: уникальная числовая метка для
оракловой функции [l]Contains()/[l]score() полнотекстового поиска
property SortByScore: Boolean; признак "включать в запрос сортировку по ранжированию score(метка)"
* данный объект предполагается только в качестве аргумента (подчинённого объекта) для объекта-выражения
  '[l]contains()'/OraFTSRange
*/

import ru.sonarplus.kernel.sqlobject.SqlObjectException;

public class OraFTSMarker extends ColumnExpression {
	public boolean sortByScore;

	public OraFTSMarker() { super(); }

	public OraFTSMarker(SqlObject owner)
			throws SqlObjectException {
		super(owner);
	}

}
