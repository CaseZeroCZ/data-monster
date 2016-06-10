package org.casezero.di;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.lucene.index.Fields;
import org.casezero.di.datamonster.CommandLineArgs;
import org.casezero.di.datamonster.es.EsClient;
import org.casezero.di.datamonster.es.FileProcessor;
import org.casezero.di.datamonster.Field;
import org.elasticsearch.action.index.IndexResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

public class DataMonster {

	private static final Logger log = LoggerFactory.getLogger(DataMonster.class);
	
    /**
     * play with ES client
     * @param args
     * @throws IOException 
     * @throws InterruptedException 
     */
    public static void main (String [] args) throws IOException, InterruptedException{
    	CommandLine cmd = new CommandLineArgs(args).parse();
    	
    	CSVReader reader = new CSVReader(new FileReader(cmd.getOptionValue("i")));
    	
        List<Field> fields = new ArrayList<Field>();
        List<String> headers = new ArrayList<String>();
    	
    	if (cmd.hasOption("f")) {
    		headers = Arrays.asList(reader.readNext());
    		for (int i = 0; i < headers.size(); i++) {
    			Field field = new Field();
    			field.setColumnNumber(i);
    			field.setOriginalFieldName(headers.get(i));
    			fields.add(field);
    		}
    	} else {
    		// Need to do something when there are no headers
    	}
    	
    	FileProcessor processor = new FileProcessor(
    			                             cmd.getOptionValue("a"),
    			                             cmd.getOptionValue("t"),
    			                             fields,
    			                             cmd,
    			                             reader
    			                         );
    	
    	processor.firstPass();
    }
}
