package org.casezero.di.datamonster;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandLineArgs {
	private static final Logger log = LoggerFactory.getLogger(CommandLineArgs.class);
	
	/**
	 * Command line options
	 */
	private Options options = new Options();
	private String[] args = null;
	private String[] requiredArgs = {"i","a","t","f"};
	
	public CommandLineArgs(String[] args) {
		this.args = args;
		
		options.addOption("h", "help", false, "Show Help");
		options.addOption("i", "input", true, "Input file to process (REQUIRED)");
		options.addOption("a", "alias", true, "ES Alias (Index) name to use (REQUIRED)");
		options.addOption("t", "type", true, "ES Type to use (REQUIRED)");
		options.addOption("f", "first", false, "Treat first line as file header (REQUIRED)");
		options.addOption("D", "delete-index", false, "Delete index before adding records THIS IS IRREVERSIBLE");
		//options.addOption("I", "input-filetype", true, "The input file type (Default: CSV)");
		//options.addOption("d", "delimiter", true, "The file delimiter if any");
		
	}
	
	public CommandLine parse() {
		CommandLineParser parser = new DefaultParser();
		
		CommandLine cmd = null;
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
		
		return cmd;
	}
	
	public void help() {
		HelpFormatter formatter = new HelpFormatter();
		
		formatter.printHelp("Main", options);
		System.exit(0);
	}
}
