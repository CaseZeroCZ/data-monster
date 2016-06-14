package org.casezero.di;

import java.io.IOException;
import org.casezero.di.datamonster.CommandLineArgs;
import org.casezero.di.datamonster.es.FileProcessor;
import org.casezero.di.datamonster.parser.DataReader;
import org.casezero.di.datamonster.parser.DataReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
