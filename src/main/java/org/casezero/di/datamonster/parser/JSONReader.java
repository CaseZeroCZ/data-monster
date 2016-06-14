package org.casezero.di.datamonster.parser;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.casezero.di.datamonster.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class JSONReader extends DataReader {
	private static final Logger log = LoggerFactory.getLogger(JSONReader.class);

	private Map<String,Field> headers = new HashMap<String,Field>();
	private List<Field> headersList;
	private String fileName;
	JsonParser parser;
	
	public JSONReader(String inputFile) throws JsonParseException, IOException {
		fileName = inputFile;
		JsonFactory f = new JsonFactory();
		if (fileName.equals("STDIN")) {
	      parser = f.createParser(System.in);
		} else {
	      parser = f.createParser(new File(fileName));
		}
	}
	
	@Override
	public String getNext() throws IOException {
		ObjectMapper mapper = new ObjectMapper();

		JsonToken next = parser.nextToken();
		if (next == JsonToken.START_OBJECT ) {
			ObjectNode node = mapper.readTree(parser);
			Map<String,Object> mapped = mapper.convertValue(node, Map.class);
			Map<String,Object> doc = new HashMap<String,Object>();
			
			// get headers and add mapped objects
			int count = 0;
			for(Entry<String, Object> m : mapped.entrySet()) {
				Field header;
				if (headers.containsKey(m.getKey())) {
					header = headers.get(m.getKey());
				} else {
				  header = new Field();
				  header.setColumnNumber(count);
				  header.setOriginalFieldName(m.getKey());
				  headers.put(m.getKey(),header);
				}
				
				doc.put(m.getKey(), m.getValue());
				
				if (prop.containsKey(header.getOriginalFieldName().toLowerCase())) {
				  addNewStructureToDoc(doc, m.getValue(), prop.getProperty(header.getOriginalFieldName().toLowerCase()), new String());
				}
			    count++;
			}
			
			return new ObjectMapper().writeValueAsString(doc);
		}
		
		if ( next == JsonToken.START_ARRAY) {
			return getNext();
		}

		return null;
	}

	@Override
	public List<Field> getHeaders() {
		headersList = new ArrayList<Field>(headers.values());
		return headersList;
	}

}
