package ru.sonarplus.kernel;

import javax.inject.Named;
import javax.inject.Singleton;
import java.sql.ResultSet;
import java.sql.Statement;

import static com.google.common.base.Verify.verify;

@Named
@Singleton
public class DbSequenceServiceOra implements DbSequenceService {

    private static final String TABLE_SEQUENCE_PREFIX = "SEQ_";

    @Override
    public long getNext(ClientSession session, String tableName) throws Exception {
        try (Statement statement = session.getConnection().createStatement();
             ResultSet rs = statement.executeQuery(
                     String.format("SELECT %s%s.NEXTVAL FROM DUAL", TABLE_SEQUENCE_PREFIX, tableName)
             )) {
            verify(rs.next());
            return rs.getLong(1);
        }
    }
}
