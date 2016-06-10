package org.casezero.di.datamonster.es;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.casezero.di.datamonster.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import au.com.bytecode.opencsv.CSVReader;

/**
 * @author smadenian
 *
 */
public class FileProcessor {
	private static final Logger log = LoggerFactory.getLogger(FileProcessor.class);
	
	private String alias;
	private String type;
	private List<Field> headers;
	private CommandLine cmd;
	
	// TODO: make this into a generic reader not just CSV and have it 
	//       return elements at a time
	private CSVReader file;
	
	private Gson gson = new Gson();
	
	private EsClient es;
	
	public FileProcessor ( String alias, String type, 
			               List<Field> headers, CommandLine cmd, 
			               CSVReader file) {
		this.alias = alias;
		this.type = type;
		this.headers = headers;
		this.cmd = cmd;
		this.file = file;
		
		// maybe place es client setup here? don't know yet
	}
	
    public void firstPass () throws IOException {
	    es = new EsClient();
	    String[] nextRow;
	    HashMap<String, String> doc = new HashMap<String, String>();
	    
	    es.setupIndex(alias);
	    
	    int lineCount = 1;
	    while((nextRow = file.readNext()) != null) {
	    	for (int i = 0; i < nextRow.length; i++) {
	    		doc.put(headers.get(i).getOriginalFieldName(), nextRow[i]);
	    	}
	        try {
	    	    es.putData(alias, type, gson.toJson(doc));
	        } catch (Exception e) {
    			log.error("Failed to load data on line "+ lineCount +":\n"+ gson.toJson(doc));
    	        log.info(e.getStackTrace().toString());
    		}
	        lineCount++;
	    }
	    
	    es.close();
    }
    
    public void secondPass () {
    	
    }

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setHeaders(List<Field> headers) {
		this.headers = headers;
	}

	public void setCmd(CommandLine cmd) {
		this.cmd = cmd;
	}

	public void setFile(CSVReader file) {
		this.file = file;
	}
}
