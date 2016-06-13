package org.casezero.di.datamonster.parser;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.casezero.di.datamonster.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

public class CSVFileReader implements DataReader {
	private static final Logger log = LoggerFactory.getLogger(CSVFileReader.class);
	
	private CSVReader file;
	private CSVReader headerFile;
	private List<Field> headers;
	private String defaultDelimiter = ",";
	
	/**
	 * Constructor. The only place where you can instantiate reading a file.
	 * If first line is a header, the headerFile will be ignored.
	 * If there is no header line, a header file (single delimited line of header names) can be 
	 * passed and will be used as header for file.
	 * 
	 * @param inputFile
	 * @param delimiter Default is a comma. Only supports single char delimiters.
	 * @param isFirstLineHeader
	 * @param inputHeaderFile must match the delimiter of original file
	 * @throws IOException 
	 */
	public CSVFileReader(String inputFile, String delimiter, boolean isFirstLineHeader, String inputHeaderFile) throws IOException {
		if (delimiter == null || delimiter.toString().isEmpty()) {
			delimiter = defaultDelimiter;
		}
		file = new CSVReader(new FileReader(inputFile), delimiter.charAt(0));
		this.headers = new ArrayList<Field>();
		List<String> stringHeaders = null;
		if ( isFirstLineHeader ) {
			stringHeaders = Arrays.asList(file.readNext());
		} else if (inputHeaderFile != null && !inputHeaderFile.isEmpty()) {
			headerFile = new CSVReader(new FileReader(inputHeaderFile), delimiter.charAt(0));
			stringHeaders = Arrays.asList(headerFile.readNext());
		}
		
		if (stringHeaders == null) {
			// error reading file
			throw new IOException("ERROR: Cannot read Header row or Header File");
		}
		
		for (int i = 0; i < stringHeaders.size(); i++) {
			Field field = new Field();
			field.setColumnNumber(i);
			field.setOriginalFieldName(stringHeaders.get(i));
			headers.add(field);
		}
	}

	public List<String> getNext() throws IOException {
		String[] row = file.readNext();
		if (row == null) {
			return null;
		}
		return new ArrayList<String>(Arrays.asList(row));
	}

	public List<Field> getHeaders() {
		return headers;
	}

}
