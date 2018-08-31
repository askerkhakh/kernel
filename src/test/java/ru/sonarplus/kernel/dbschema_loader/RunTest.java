package ru.sonarplus.kernel.dbschema_loader;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import ru.sonarplus.kernel.dbschema.DbSchemaSpec;
import ru.sonarplus.kernel.dbschema_loader.Loader;

public class RunTest {

	public RunTest() {
		// TODO Auto-generated constructor stub
	}
    @Test
    public void load() throws Exception {
		DbSchemaSpec result = Loader.loadFromFile("C:\\projects\\db\\tor.db");
    	assertTrue(result != null);
    }

    @Test
    public void loadWithUserDBD() throws Exception {
		DbSchemaSpec result = Loader.loadFromFile("C:\\projects\\db\\tor.db");
    	assertTrue(result != null);
    	Loader.loadFromFileToDBSchemaSpec("C:\\projects\\db\\user_tor.db", result, false);
    }
}
