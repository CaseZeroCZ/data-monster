package org.casezero.di.datamonster.es;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.casezero.di.datamonster.CommandLineArgs;
import org.casezero.di.datamonster.Field;
import org.casezero.di.datamonster.parser.DataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;


/**
 * @author smadenian
 *
 */
public class FileProcessor {
	private static final Logger log = LoggerFactory.getLogger(FileProcessor.class);
	
	private String alias;
	private String type;
	private CommandLineArgs cmd;
	
	// TODO: make this into a generic reader not just CSV and have it 
	//       return elements at a time
	private DataReader file;
	
	private Gson gson = new Gson();
	private EsClient es;
	private Properties prop = new Properties();
	private Properties propDataTypes = new Properties();
	private HashMap<String,Object> mappedVars = new HashMap<String,Object>();
	
	
	public FileProcessor ( CommandLineArgs cmd, 
			               DataReader file) {
		this.alias = cmd.getOptionValue(CommandLineArgs.INDEX_ALIAS);
		this.type = cmd.getOptionValue(CommandLineArgs.INDEX_TYPE);
		this.cmd = cmd;
		this.file = file;
		
		// maybe place es client setup here? don't know yet
	}
	
	private void loadProps() throws IOException {
		// TODO: move this to DataMonster where headers are parsed?
    	InputStream mappings = this.getClass().getResourceAsStream("/mappings.properties");
    	prop.load(mappings);
    	
    	InputStream dataTypes = this.getClass().getResourceAsStream("/mapping-data-type.properties");
    	propDataTypes.load(dataTypes);
	}
	
    public void firstPass () throws InterruptedException, IOException {
	    // set up
    	es = new EsClient();
    	loadProps();
    	String alias = this.alias +"_firstpass";
	    
	    List<String> nextRow;
	    HashMap<String, Object> doc = new HashMap<String, Object>();
	    
	    if (es.hasIndex(alias)) {
	    	es.deleteIndex(alias);
	    }
	    es.setupIndex(alias);
	    
	    int lineCount = 1;
	    while((nextRow = file.getNext()) != null) {
	    	List<Field> headers = file.getHeaders();
	    	for (int i = 0; i < nextRow.size(); i++) {
	    		doc.put(headers.get(i).getOriginalFieldName(), nextRow.get(i));
	    		if (prop.containsKey(headers.get(i).getOriginalFieldName().toLowerCase())) {
	    			
	    			addNewStructureToDoc(doc, nextRow.get(i), prop.getProperty(headers.get(i).getOriginalFieldName().toLowerCase()), new String());
	    		}
	    	}
	        try {
	    	    es.bulkAdd(alias, type, gson.toJson(doc));
	        } catch (Exception e) {
    			log.error("Failed to load data on line "+ lineCount +":\n"+ gson.toJson(doc));
    	        log.info(e.getStackTrace().toString());
    		}
	        lineCount++;
	    }
	    
	    es.bulkFlush();
    }
    
    private void addNewStructureToDoc( HashMap<String, Object> doc, 
    		                           Object value, 
    		                           String remainingPath,
    		                           String path) {
    	String key;
    	
    	if (remainingPath.contains(".")) {
    		int indexOfDot = remainingPath.indexOf('.');
    		key = remainingPath.substring(0, indexOfDot);
    		if (!doc.containsKey(key)) {
    			doc.put(key, new HashMap<String,Object>());
    		}
    		
    		path = joinPath(path, key);
    		
    		addNewStructureToDoc( (HashMap<String, Object>) doc.get(key), value, remainingPath.substring(indexOfDot + 1), path);

    	} else {
    		key = remainingPath;
    		doc.put(key, value);
    		
    		path = joinPath(path, key);
    	}
    	
    	// check if path has a mapping and add it
    	if(propDataTypes.containsKey(path)) {
    	    String[] pathNames = path.split("\\.");
    	    if (pathNames.length == 0)
    	    	return;
    	    HashMap<String, Object> currMap = mappedVars;
    	    
    	    for(String pathName : pathNames) {
    	    	if (currMap.containsKey("properties")) {
    	    		currMap = (HashMap<String, Object>) currMap.get("properties");
    	    	}
    	    	if (!currMap.containsKey(pathName)) {
    	    		currMap.put(pathName, new HashMap<String,Object>());
    	    		((HashMap<String,Object>) currMap.get(pathName)).put("properties", new HashMap<String,Object>());
    	    	}
    	    	currMap = (HashMap<String, Object>) currMap.get(pathName);
    	    }
    	    
    	    currMap.put("type", propDataTypes.getProperty(path));
    	    currMap.remove("properties");
    	}
    }
    
    private String joinPath(String path, String key) {
    	if (path.length() != 0)
			path = path.concat(".");
		path = path.concat(key);
		return path;
    }
    
    public void secondPass () throws InterruptedException {
    	// if we are asked to delete index first
	    if (cmd.hasOption(CommandLineArgs.DELETE_INDEX)) {
	    	if (es.hasIndex(alias))
	    	    es.deleteIndex(alias);
		    es.setupIndex(alias);
	    }
	    
    	
    	// fix mapping hashmap mappings->alias->properties->mappedVars
    	HashMap<String,Object> propertiesMap = new HashMap<String, Object>();
    	propertiesMap.put("properties", mappedVars);
    	
    	HashMap<String, Object> typeMap = new HashMap<String, Object>();
    	typeMap.put(type, propertiesMap);
    	
    	//HashMap<String, Object> mappings = new HashMap<String,Object>();
    	//mappings.put("mappings", typeMap);
    	
    	log.debug("MAPPINGS: "+ gson.toJson(typeMap));
    	
    	es.setupMapping(alias, type, gson.toJson(typeMap));
    	
    	es.reindex(alias+"_firstpass", alias);
    	
    	//es.deleteIndex(alias+"_firstpass");
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
