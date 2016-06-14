package org.casezero.di.datamonster.parser;

import java.io.IOException;
import org.casezero.di.datamonster.CommandLineArgs;

public class DataReaderFactory {
    public static DataReader getDataReader(CommandLineArgs cmd) throws IOException {
    	if (cmd == null) {
    		return null;
    	}
    	if (cmd.getOptionValue(CommandLineArgs.INPUT_FILE_TYPE) == null ||
    			cmd.getOptionValue(CommandLineArgs.INPUT_FILE_TYPE).isEmpty()) {
    		throw new UnsupportedOperationException("No File Type Specified to process");
    	}
    	
    	switch(cmd.getOptionValue(CommandLineArgs.INPUT_FILE_TYPE).toLowerCase()) {
    	  case "csv":
    		  DataReader csvReader = new CSVFileReader(
    				      cmd.getOptionValue(CommandLineArgs.INPUT_FILE),
    				      cmd.getOptionValue(CommandLineArgs.FILE_DELIMITER),
    				      cmd.hasOption(CommandLineArgs.IS_FIRST_HEADER),
    				      cmd.getOptionValue(CommandLineArgs.HEADER_FILE)
    				  );
    		  return csvReader;
    	  case "json":
    		  DataReader jsonReader = new JSONFileReader(cmd.getOptionValue(CommandLineArgs.INPUT_FILE));
    		  return jsonReader;
    	  default:
    		  throw new UnsupportedOperationException("file type "+ cmd.getOptionValue(CommandLineArgs.INPUT_FILE_TYPE) +" is not supported at this time");
    	}
    }
}
