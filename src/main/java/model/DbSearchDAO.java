package model;

import com.datastax.driver.core.*;

import datacontroller.CassandraConnector;

public class DbSearchDAO {

	public Session session;
	public String cqlStatement;
	
	public DbSearchDAO() {
		CassandraConnector ccon = new CassandraConnector();
		this.session = ccon.Connect();
	}
	
	public String getAllRecords() {
		cqlStatement = "select JSON * from demo.dublin_bikes_json2;";
		return getResults(cqlStatement);
	}
	
	public String getRecordsById(int id) {
		cqlStatement = "select JSON * from demo.dublin_bikes_json2 where number="+id+";";
		return getResults(cqlStatement);
	}
	
	public String getRecordsByTime(String t1, String t2) {
		cqlStatement = "select JSON * from demo.dublin_bikes_json2 WHERE last_update >='"+t1+"' AND last_update <= '"+t2+"' ALLOW FILTERING;";
		return getResults(cqlStatement);
	}
	
	public String getRecordsByIdAndTime(int id, String t1, String t2) {
		cqlStatement = "select JSON * from demo.dublin_bikes_json2 WHERE number="+id+" AND last_update >='"+t1+"' AND last_update <= '"+t2+"' ALLOW FILTERING;";
		return getResults(cqlStatement);
	}
	
	public String getAverageByIdAndTime(int id, String t1, String t2) {
		cqlStatement = "select JSON number, avg(available_bikes) as avg_available_bikes,bike_stands, avg(available_bike_stands) as avg_available_bike_stands,position,address,name from demo.dublin_bikes_json2 WHERE  number="+id+" AND  last_update >='"+t1+"' AND last_update <= '"+t2+"' ALLOW FILTERING;";
		return getResults(cqlStatement);
	}
	
	public String getResults(String cqlStatement) {
	    
		int count =0;
	    String jsonStns = "[";
	    for (Row row : session.execute(cqlStatement)) {
	    	if(count>0)
	    	jsonStns = jsonStns + "," +row.getString("[json]");
	    	else
	    		jsonStns ="{" +"\"dbikesdata\":["+ row.getString("[json]");
	    	count++;
		}
	    jsonStns = jsonStns+"]}";
	    return jsonStns;
	}

}
