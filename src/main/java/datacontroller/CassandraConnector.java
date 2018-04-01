package datacontroller;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraConnector {
	
	private static final Logger log = LoggerFactory.getLogger(CassandraConnector.class);
	public static Session session;
	
	public Session Connect() {
		
		Properties prop = new Properties();
		InputStream input = null;
		
		try {
			
			input = new FileInputStream("config.properties");

			// load the properties file
			prop.load(input);
			Cluster cluster;

		    cluster = Cluster.builder()
		            .addContactPoint(prop.getProperty("csdraIP"))
		            .withPort(Integer.parseInt(prop.getProperty("csdraPort")))
		            .withAuthProvider(new PlainTextAuthProvider(prop.getProperty("csdraUserId"),prop.getProperty("csdraPwd")))
		            .build();

		    Session session = cluster.connect("demo");
		    return session;
		    
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return session;
	}
}
