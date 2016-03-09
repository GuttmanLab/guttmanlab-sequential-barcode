package testing;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;

import org.apache.log4j.Logger;

import contact.SAMFragmentCluster;
import contact.function.FragmentClusterFunction;
import contact.function.SAMRecordPredicate;
import contact.iterator.SAMFragmentClusterIterator;
import guttmanlab.core.coordinatespace.CoordinateSpace;
import guttmanlab.core.util.CommandLineParser;

public class TestSAMFragmentClusterIterator {
	
	private static Logger logger = Logger.getLogger(TestSAMFragmentClusterIterator.class.getName());
	
	public static void main(String[] args) throws IOException {
		
		CommandLineParser p = new CommandLineParser();
		p.addStringArg("-b", "Barcode-sorted bam file", true);
		p.addStringArg("-c", "Chromosome size file", true);
		p.addStringArg("-o", "Output file of clusters", true);
		p.parse(args);
		String bam = p.getStringArg("-b");
		String chr = p.getStringArg("-c");
		String out = p.getStringArg("-o");
		SAMFragmentClusterIterator iter = new SAMFragmentClusterIterator(bam, new CoordinateSpace(chr), Collections.singletonList(new SAMRecordPredicate.PrimaryMapping()));
		FileWriter writer = new FileWriter(out);
		while(iter.hasNext()) {
			SAMFragmentCluster cluster = iter.next();
			if(cluster.getNumBarcodes() < 4) continue;
			writer.write(cluster.toString(FragmentClusterFunction.tabDelimitedString()) + "\n");
		}
		writer.close();
		iter.close();
		
		logger.info("");
		logger.info("All done");
		
	}
	
}
