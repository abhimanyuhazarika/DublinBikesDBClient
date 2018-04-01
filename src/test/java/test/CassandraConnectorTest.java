package test;


import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.*;

import datacontroller.CassandraConnector;
import com.datastax.driver.core.*;
public class CassandraConnectorTest {

	@Test
	public void testConnect() {
		//fail("Not yet implemented");
		CassandraConnector ccon = new CassandraConnector();
		Session session = ccon.Connect();
		
		Cluster cluster;
	    cluster = Cluster.builder()
	            .addContactPoint("54.191.120.32")
	            .withPort(9042)
	            .withAuthProvider(new PlainTextAuthProvider("cassandra","cassandra"))
	            .build();

	    Session testSession = cluster.connect("demo");
		final Metadata metadata = cluster.getMetadata();
		assertEquals("Test Cluster", metadata.getClusterName());
	}

}
