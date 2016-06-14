package org.casezero.di.datamonster;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableMap;

public class CommandLineArgs {
	private static final Logger log = LoggerFactory.getLogger(CommandLineArgs.class);
	
	/**
	 * human readable option names
	 */
	public static final String HELP = "h";
	public static final String INPUT_FILE = "i";
	public static final String INDEX_ALIAS = "a";
	public static final String INDEX_TYPE = "t";
	public static final String IS_FIRST_HEADER = "f";
	public static final String DELETE_INDEX = "D";
	public static final String INPUT_FILE_TYPE = "I";
	public static final String FILE_DELIMITER = "d";
	public static final String HEADER_FILE = "H";
	
	/**
	 * Command line options
	 */
	private Options options = new Options();
	private String[] args = null;
	private String[] requiredArgs = {INPUT_FILE, INDEX_ALIAS, INDEX_TYPE};
	private Map<String,String> defaults = ImmutableMap.of(
			    FILE_DELIMITER, ",",
			    INPUT_FILE_TYPE, "CSV"
			);
	private CommandLine cmd;
	
	public CommandLineArgs(String[] args) {
		this.args = args;
		
		options.addOption(HELP, "help", false, "Show Help");
		options.addOption(INPUT_FILE, "input", true, "Input file to process or type STDIN to use stdin from command line (REQUIRED)");
		options.addOption(INDEX_ALIAS, "alias", true, "ES Alias (Index) name to use (REQUIRED)");
		options.addOption(INDEX_TYPE, "type", true, "ES Index Type name to use (REQUIRED)");
		options.addOption(IS_FIRST_HEADER, "first", false, "Treat first line as file header (REQUIRED)");
		options.addOption(DELETE_INDEX, "delete-index", false, "Delete index before adding records THIS IS IRREVERSIBLE");
		options.addOption(INPUT_FILE_TYPE, "input-filetype", true, "The input file type (Default: CSV, Supported types: CSV)");
		options.addOption(FILE_DELIMITER, "delimiter", true, "The file delimiter if any");
		options.addOption(HEADER_FILE, "header-file", true, "The header file if first line of input file is not a header");
	}
	
	public CommandLineArgs parse() {
		CommandLineParser parser = new DefaultParser();
		
		cmd = null;
		try {
			cmd = parser.parse(options, args);
			
			if (cmd.hasOption("h")) 
				help();
			
			for (String req : requiredArgs) {
				if (!cmd.hasOption(req)) {
					log.error("Missing required argument -"+ req);
					help();
				}
			}
			
		} catch (ParseException e) {
		    log.error("Failed to parse command line options");
		    help();
	    }
		
		return this;
	}
	
	public void help() {
		HelpFormatter formatter = new HelpFormatter();
		
		formatter.printHelp("Main", options);
		System.exit(0);
	}
	
	public String getOptionValue(String key) {
		String value = null;
		
		if (cmd != null) {
			value = cmd.getOptionValue(key);
		}
		
		if ( defaults.containsKey(key) && value == null) {
			value = defaults.get(key);
		}
		
		return value;
	}
	
	public boolean hasOption(String key) {
		return cmd.hasOption(key);
	}
}
