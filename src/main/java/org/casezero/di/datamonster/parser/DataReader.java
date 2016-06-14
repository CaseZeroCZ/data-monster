package org.casezero.di.datamonster.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.casezero.di.datamonster.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Splitter;

public abstract class DataReader {
	private static final Logger log = LoggerFactory.getLogger(DataReader.class);
	
	protected Properties prop = new Properties();
	protected Properties propDataTypes = new Properties();
	private HashMap<String,Object> mappedVars = new HashMap<String,Object>();
	
	public DataReader() throws IOException {
		InputStream mappings = this.getClass().getResourceAsStream("/mappings.properties");
    	prop.load(mappings);
    	
    	InputStream dataTypes = this.getClass().getResourceAsStream("/mapping-data-type.properties");
    	propDataTypes.load(dataTypes);
	}
	
    protected void addNewStructureToDoc( Map<String, Object> doc, 
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
        
        Map<String,String> mappings = Splitter.on(",")
        		.omitEmptyStrings()
        		.trimResults()
        		.withKeyValueSeparator("|")
        		.split(propDataTypes.getProperty(path));
        
        for (Entry<String, String> mapping : mappings.entrySet()) {
        	currMap.put(mapping.getKey(), mapping.getValue());
        } 
        
        currMap.remove("properties");
      }
    }

    private String joinPath(String path, String key) {
      if (path.length() != 0)
        path = path.concat(".");
      path = path.concat(key);
      return path;
    }
    
    public HashMap<String, Object> getMappedVars() {
    	return this.mappedVars;
    }
	
    public abstract String getNext() throws IOException;
    public abstract List<Field> getHeaders();
}
