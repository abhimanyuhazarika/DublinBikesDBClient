package test;

import static org.junit.Assert.*;

import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;

import model.DbSearchDAO;
import org.json.*;

public class DbSearchDAOTest {

    Cluster cluster = Cluster.builder()
            .addContactPoint("54.191.120.32")
            .withPort(9042)
            .withAuthProvider(new PlainTextAuthProvider("cassandra","cassandra"))
            .build();

    Session testSession = cluster.connect("demo");

	@Test
	public void testGetAllRecords() {
		DbSearchDAO dbSearchDAO = new DbSearchDAO();
		String allRecords = dbSearchDAO.getAllRecords();
		String cqlStatement = "select JSON * from demo.dublin_bikes_json2;";
		String testAllRecords = dbSearchDAO.getResults(cqlStatement);
		assertEquals(testAllRecords, allRecords);
	}

	@Test
	public void testGetRecordsById() {
		DbSearchDAO dbSearchDAO = new DbSearchDAO();
		String allRecords = dbSearchDAO.getRecordsById(23);
		String cqlStatement = "select JSON * from demo.dublin_bikes_json2 where number=23;";
		String testAllRecords = dbSearchDAO.getResults(cqlStatement);
		assertEquals(testAllRecords, allRecords);
	}

	@Test
	public void testGetRecordsByTime() {
		DbSearchDAO dbSearchDAO = new DbSearchDAO();
		String allRecords = dbSearchDAO.getRecordsByTime("2018-03-25","2018-03-26");
		String cqlStatement = "select JSON * from demo.dublin_bikes_json2 WHERE last_update >='2018-03-25' AND last_update <= '2018-03-26' ALLOW FILTERING;";
		String testAllRecords = dbSearchDAO.getResults(cqlStatement);
		assertEquals(testAllRecords, allRecords);
	}

	@Test
	public void testGetRecordsByIdAndTime() {
		DbSearchDAO dbSearchDAO = new DbSearchDAO();
		String allRecords = dbSearchDAO.getRecordsByIdAndTime(23,"2018-03-25","2018-03-26");
		String cqlStatement = "select JSON * from demo.dublin_bikes_json2 WHERE number =23 AND last_update >='2018-03-25' AND last_update <= '2018-03-26' ALLOW FILTERING;";
		String testAllRecords = dbSearchDAO.getResults(cqlStatement);
		assertEquals(testAllRecords, allRecords);
	}

	@Test
	public void testGetAverageByIdAndTime() {
		DbSearchDAO dbSearchDAO = new DbSearchDAO();
		String allRecords = dbSearchDAO.getRecordsByIdAndTime(23,"2018-03-25","2018-03-26");
		String cqlStatement = "select JSON number, avg(available_bikes) as avg_available_bikes,bike_stands, avg(available_bike_stands) as avg_available_bike_stands,position,address,name from demo.dublin_bikes_json2 WHERE  number=23 AND  last_update >='2018-03-25' AND last_update <= '2018-03-26' ALLOW FILTERING;";
		String testAllRecords = dbSearchDAO.getResults(cqlStatement);
		assertEquals(testAllRecords, allRecords);
	}

	@Test
	public void testGetResults() {
		DbSearchDAO dbSearchDAO = new DbSearchDAO();
		String cqlStatement = "select JSON number from demo.dublin_bikes_json2 where number=23 limit 1;";
		String testAllRecords = dbSearchDAO.getResults(cqlStatement);
		assertTrue(isJSONValid(testAllRecords));
	}

	public boolean isJSONValid(String test) {
	    try {
	        new JSONObject(test);
	    } catch (JSONException ex) {
	        try {
	            new JSONArray(test);
	        } catch (JSONException ex1) {
	            return false;
	        }
	    }
	    return true;
	}
	

}
