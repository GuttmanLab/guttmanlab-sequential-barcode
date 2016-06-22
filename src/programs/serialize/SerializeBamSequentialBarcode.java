package programs.serialize;

import java.io.IOException;

import org.apache.log4j.Logger;

import util.Filters;
import guttmanlab.core.serialize.sam.SerializeBam;
import guttmanlab.core.util.CommandLineParser;

public class SerializeBamSequentialBarcode {
	
	private static Logger logger = Logger.getLogger(SerializeBamSequentialBarcode.class.getName());
	
	public static void main(String[] args) {
		
		CommandLineParser p = new CommandLineParser();
		p.addStringArg("-b", "Bam file", true);
		p.addStringArg("-s", "Avro schema file", true);
		p.addStringArg("-o", "Output avro file", true);
		p.addBooleanArg("-df", "Use default SAM record filters", true);
		p.parse(args);

		String bam = p.getStringArg("-b");
		String schema = p.getStringArg("-s");
		String out = p.getStringArg("-o");
		boolean useDefaultFilters = p.getBooleanArg("-df");
		
		if(useDefaultFilters) {
			try {
				SerializeBam.serialize(schema, bam, out, Filters.defaultSamFilters());
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		} else {
			throw new IllegalArgumentException("Currently the only supported option is to use default SAM record filters.");
		}
		
		logger.info("");
		logger.info("All done.");
		
	}
	
}
