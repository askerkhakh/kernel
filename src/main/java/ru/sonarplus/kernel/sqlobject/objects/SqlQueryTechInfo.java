package ru.sonarplus.kernel.sqlobject.objects;

import java.util.ArrayList;
import java.util.List;

public class SqlQueryTechInfo {
    public List<Object> oraFTSMarkers = new ArrayList<Object>();

	public SqlQueryTechInfo() {
	}
	
	public void addOraFtsMarker(Object marker) {
		if ((marker != null) && (oraFTSMarkers.indexOf(marker) < 0)) {
			oraFTSMarkers.add(marker);
		}
	}
	
	public void addOraFtsMarkers(List<Object> markers) {
		if (markers != null) {
			for (Object item: markers) {
				addOraFtsMarker(item);
			}
			markers.clear();
		}
	}
	
	public void assignFrom(SqlQuery query) {
		if (query != null) {
			addOraFtsMarkers(query.techInfo.oraFTSMarkers);
			query.techInfo.freeOraFtsMarkers();
		}
	}
	
	public void freeOraFtsMarkers() {
		oraFTSMarkers.clear();
	}

}
