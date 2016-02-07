package edu.mpcs53013.serialCrime;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import edu.uchicago.mpcs53013.crimeSummary.CrimeSummary;


public abstract class CrimeSummaryProcessor {
	static class MissingDataException extends Exception {

	    public MissingDataException(String message) {
	        super(message);
	    }

	    public MissingDataException(String message, Throwable throwable) {
	        super(message, throwable);
	    }

	}
	
	void processLine(String line, File file) throws IOException {
		try {
			processCrimeSummary(crimeFromLine(line), file);
		} catch(MissingDataException e) {
			// Just ignore lines with missing data
		}
	}

	abstract void processCrimeSummary(CrimeSummary summary, File file) throws IOException;
	BufferedReader getFileReader(File file) throws FileNotFoundException, IOException {
		if(file.getName().endsWith(".gz"))
			return new BufferedReader
					     (new InputStreamReader
					    		 (new GZIPInputStream
					    				 (new FileInputStream(file))));
		return new BufferedReader(new InputStreamReader(new FileInputStream(file)));
	}
	
	void processCrimeFile(File file) throws IOException {		
		BufferedReader br = getFileReader(file);
		br.readLine(); // Discard header
		String line;

		while((line = br.readLine()) != null) {
			try{
				processLine(line, file);
			} catch (NumberFormatException e){}
		}
	}

	void processCrimeDirectory(String directoryName) throws IOException {
		File directory = new File(directoryName);
		File[] directoryListing = directory.listFiles();
		for(File noaaFile : directoryListing)
			processCrimeFile(noaaFile);
	}
	
	CrimeSummary crimeFromLine(String line) throws NumberFormatException, MissingDataException {

		String regex = "\"([^\"]*)\",|([^,]*),";
		Matcher m = Pattern.compile(regex).matcher(line);
	
		String[] datetime = {""};
		String block = "";
		String desc = "";
		Short ward = 0;
		
		
		for(int i = 0; i < 14; ++i){
			m.find();
			if(i == 2)
				datetime = m.group(2).split("[ :/]");
			else if(i == 3)
				block = m.group(2);
			else if(i == 5)
				desc = m.group(2);
			else if(i == 12)
				ward = Short.parseShort(m.group(2));
				
		}
		
		CrimeSummary summary 
		= new CrimeSummary(Byte.parseByte(datetime[0]),
							Byte.parseByte(datetime[1]),
							Short.parseShort(datetime[2]),
							Byte.parseByte(datetime[3]),
							Byte.parseByte(datetime[4]),
							datetime[6],
							block,
							desc,
							ward);			
		return summary;
	}

}
