package ru.sonarplus.kernel.dbvalues;

import java.util.Date;

import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_DATE;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_LARGEINT;
import static ru.sonarplus.kernel.dbschema.FieldTypeId.tid_TIME;

/**
 * Created by stepanov on 30.11.2017.
 */
public class TechFields extends DBRow {
    public static final String lfAddDate = "DATAPOST_";
    public static final String lfAddTime = "VREMYAPOST_";
    public static final String lfChangeDate = "DATAIZMEN_";
    public static final String lfChangeTime = "VREMYAIZMEN";
    public static final String lfChangeUser = "KTOVVEL";

    public DBField addDate = field(lfAddDate, tid_DATE);
    public DBField addTime = field(lfAddTime, tid_TIME);
    public DBField changeDate = field(lfChangeDate, tid_DATE);
    public DBField changeTime = field(lfChangeTime, tid_TIME);
    public DBField changeUser = field(lfChangeUser, tid_LARGEINT);

    public TechFields(DBRow owner) {
        super(owner, true);
    }

    public static TechFields getForInsert(DBRow owner, Long changeUser) {
        TechFields techFields = new TechFields(owner);
        Date now = new Date();
        techFields.addDate.setDate(now);
        techFields.addTime.setDate(now);
        techFields.changeDate.setDate(now);
        techFields.changeTime.setDate(now);
        techFields.changeUser.setLong(changeUser);
        return techFields;
    }

    public static TechFields getForUpdate(DBRow owner, Long changeUser) {
        TechFields techFields = new TechFields(owner);
        Date now = new Date();
        techFields.changeDate.setDate(now);
        techFields.changeTime.setDate(now);
        techFields.changeUser.setLong(changeUser);
        return techFields;
    }

}
