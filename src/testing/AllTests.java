package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

import org.junit.AfterClass;






public class AllTests {

	private static KVServer server;

	static {
		try {
			System.out.println("Server loop here");
			new LogSetup("logs/testing/test.log", Level.ERROR);
			server = new KVServer(1427, 10, "FIFO");
			new Thread(server).start();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class); 
		clientSuite.addTestSuite(AdditionalTest.class);
		//clientSuite.addTestSuite(PerformanceTest.class);


		return clientSuite;
	}

	@AfterClass
    public static void tearDownClass() {
        if (server != null) {
            server.close();
        }
	}
	
	
}
