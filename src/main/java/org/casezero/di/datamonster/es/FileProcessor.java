package org.casezero.di.datamonster.es;

import java.io.IOException;
import java.util.HashMap;

import org.casezero.di.datamonster.CommandLineArgs;
import org.casezero.di.datamonster.parser.DataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * @author smadenian
 *
 */
public class FileProcessor {
	private static final Logger log = LoggerFactory.getLogger(FileProcessor.class);
	
	private String alias;
	private String type;
	private CommandLineArgs cmd;
	private DataReader file;
	private EsClient es;
	
	private String firstPassName = "_firstpass";
	
	
	public FileProcessor ( CommandLineArgs cmd, 
			               DataReader file) {
		this.alias = cmd.getOptionValue(CommandLineArgs.INDEX_ALIAS);
		this.type = cmd.getOptionValue(CommandLineArgs.INDEX_TYPE);
		this.cmd = cmd;
		this.file = file;
		
		// maybe place es client setup here? don't know yet
	}

	
    public void firstPass () throws InterruptedException, IOException {
	    // set up
    	es = new EsClient();
    	String alias = this.alias + firstPassName;
	    
	    String nextRow;
	    
	    if (es.hasIndex(alias) && cmd.hasOption(CommandLineArgs.DELETE_INDEX)) {
	    	es.deleteIndex(alias);
	    }
	    es.setupIndex(alias);
	    
	    int lineCount = 1;
	    while((nextRow = file.getNext()) != null) {
	        try {
	    	    es.bulkAdd(alias, type, nextRow);
	        } catch (Exception e) {
    			log.error("Failed to load data on line "+ lineCount +":\n"+ nextRow);
    	        log.info(e.getStackTrace().toString());
    		}
	        lineCount++;
	    }
	    
	    es.bulkFlush();
    }
    
    public void secondPass () throws InterruptedException, JsonProcessingException {
    	// if we are asked to delete index first
	    if (cmd.hasOption(CommandLineArgs.DELETE_INDEX)) {
	    	if (es.hasIndex(alias))
	    	    es.deleteIndex(alias);
		    es.setupIndex(alias);
	    }
	    
    	
	    if (file.getMappedVars().size() > 0) {
	    	// fix mapping hashmap mappings->alias->properties->mappedVars
	    	HashMap<String,Object> propertiesMap = new HashMap<String, Object>();
	    	propertiesMap.put("properties", file.getMappedVars());
	    	
	    	HashMap<String, Object> typeMap = new HashMap<String, Object>();
	    	typeMap.put(type, propertiesMap);
	    	
	    	//HashMap<String, Object> mappings = new HashMap<String,Object>();
	    	//mappings.put("mappings", typeMap);
	    	
	    	log.debug("MAPPINGS: "+ new ObjectMapper().writeValueAsString(typeMap));
	    	
	    	es.setupMapping(alias, type, new ObjectMapper().writeValueAsString(typeMap));
	    }
    	
    	es.reindex(alias + firstPassName, type, alias);
    	
    	//es.deleteIndex(alias+firstPassName);
    }
    
    public void shutdown() throws InterruptedException {
    	es.close();
    }

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setCmd(CommandLineArgs cmd) {
		this.cmd = cmd;
	}

	public void setFile(DataReader file) {
		this.file = file;
	}

}
