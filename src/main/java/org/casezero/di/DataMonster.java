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
import org.casezero.di.datamonster.parser.CSVFileReader;
import org.casezero.di.datamonster.parser.DataReader;
import org.casezero.di.datamonster.parser.DataReaderFactory;
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
    	CommandLineArgs cmd = new CommandLineArgs(args).parse();
    	
    	DataReader reader = DataReaderFactory.getDataReader(cmd);
    	
    	FileProcessor processor = new FileProcessor(
    			                             cmd,
    			                             reader
    			                         );
    	
    	processor.firstPass();
    	processor.secondPass();
    	processor.shutdown();
    }
}
