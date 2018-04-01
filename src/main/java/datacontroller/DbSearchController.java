package datacontroller;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import model.DbSearchDAO;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

@RestController
public class DbSearchController {

    private final AtomicLong counter = new AtomicLong();
    private static final Logger log = LoggerFactory.getLogger(DbSearchController.class);

    @RequestMapping("/dbdataclient")
    public JSONObject dbdataclient(@RequestParam(value="id", defaultValue="None") String id, @RequestParam(value="tp1",defaultValue="None") String tp1, @RequestParam(value="tp2",defaultValue="None") String tp2,@RequestParam(value="avg",defaultValue="None") String avg) throws ParseException, IOException {
    	
    	String result = null;
    	DbSearchDAO db = new DbSearchDAO();
    	if(!tp1.equals("None") && !tp2.equals("None") && !avg.equals("None") && !id.equals("None"))
    	{
    		result = db.getAverageByIdAndTime(Integer.parseInt(id), tp1, tp2);
    		log.info("INSIDE SEARCH average BY TIME PERIOD");
    	}
    	else if(!id.equals("None") && !tp1.equals("None") && !tp2.equals("None"))
    	{
    		result = db.getRecordsByIdAndTime(Integer.parseInt(id), tp1, tp2);
    		log.info("INSIDE SEARCH BY ID & TP");
    	}
    	else if(!id.equals("None"))
    	{
    		result = db.getRecordsById(Integer.parseInt(id));
    		log.info("INSIDE SEARCH BY ID");
    	}
    	else if(!tp1.equals("None"))
    	{
    		result = db.getRecordsByTime(tp1, tp2);
    		log.info("INSIDE SEARCH BY TIME PERIOD");
    	}
    	else
    	{
    		result = db.getAllRecords();
    		log.info("INSIDE SEARCH ALL");
    	}

    	@SuppressWarnings("deprecation")
		JSONParser parser = new JSONParser();
    	JSONObject json = (JSONObject) parser.parse(result);
    	return json;
    } 
}
