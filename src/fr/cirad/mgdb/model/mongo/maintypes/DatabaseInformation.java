package fr.cirad.mgdb.model.mongo.maintypes;

import java.util.Date;

public class DatabaseInformation {
	public static final String FIELDNAME_LAST_MODIFICATION = "lastModification";
	public static final String FIELDNAME_RESTORE_DATE = "restoreDate";

	private Date lastModification = null;
	private Date restoreDate = null;
	
	public Date getLastModification() {
		return lastModification;
	}

	public void setLastModification(Date lastModification) {
		this.lastModification = lastModification;
	}
	
	public Date getRestoreDate() {
		return restoreDate;
	}

	public void setRestored(Date restoreDate) {
		this.restoreDate = restoreDate;
	}
}
