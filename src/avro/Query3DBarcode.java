package avro;

import guttmanlab.core.util.CommandLineParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.util.CloseableIterator;

import org.apache.log4j.Logger;

import programs.BarcodedBamWriter;
import guttmanlab.core.annotationcollection.FeatureCollection;
import broad.core.parser.StringParser;
import sequentialbarcode.BarcodeSequence;
import guttmanlab.core.serialize.sam.AvroSamRecord;
import guttmanlab.core.serialize.sam.AvroSamStringIndex;


/**
 * 
 * @author prussell
 *
 */
public class Query3DBarcode {
	
	@SuppressWarnings("unused")
	private static Logger logger = Logger.getLogger(Query3DBarcode.class.getName());
	private static int MIN_NUM_BARCODES_PER_FRAGMENT = 0;
	
	private static boolean numBarcodesOk(String barcodeString) {
		BarcodeSequence barcodes = BarcodeSequence.fromSamAttributeString(barcodeString);
		return barcodes.getNumBarcodes() >= MIN_NUM_BARCODES_PER_FRAGMENT;
	}
		
	/**
	 * Get the collection of barcode attributes for all reads overlapping a region
	 * @param samReader SAM reader for bam file of read mappings
	 * @param chr Region chromosome
	 * @param start Region start
	 * @param end Region end
	 * @return Collection of barcodes for reads overlapping the region. Each barcode only included once.
	 */
	public static Collection<String> getBarcodes(SAMFileReader samReader, String chr, int start, int end) {
		Collection<String> rtrn = new HashSet<String>();
		SAMRecordIterator iter = samReader.queryOverlapping(chr, start, end);
		while(iter.hasNext()) {
			SAMRecord record = iter.next();
			String barcodeString = (String) record.getAttribute(BarcodedBamWriter.BARCODES_SAM_TAG);
			if(numBarcodesOk(barcodeString)) {
				rtrn.add(barcodeString);
			}
		}
		iter.close();
		return rtrn;
	}
	
	/**
	 * Get the collection of read IDs for all reads overlapping a region
	 * @param samReader SAM reader for bam file of read mappings
	 * @param chr Region chromosome
	 * @param start Region start
	 * @param end Region end
	 * @return Collection of read IDs for all reads overlapping the region. Each ID only included once.
	 */
	public static Collection<String> getReadIDs(SAMFileReader samReader, String chr, int start, int end) {
		Collection<String> rtrn = new HashSet<String>();
		SAMRecordIterator iter = samReader.queryOverlapping(chr, start, end);
		while(iter.hasNext()) {
			SAMRecord record = iter.next();
			String id = record.getReadName();
			rtrn.add(id);
		}
		iter.close();
		return rtrn;
	}
	
	/**
	 * Print the records to std out
	 * @param records Records to print
	 */
	private static void printRecords(FeatureCollection<AvroSamRecord> records) {
		for(AvroSamRecord record : records) {
			String line = record.getName() + "\t";
			line += record.toUCSC() + "\t";
			line += record.getAttribute("tagXB") + "\t";
			System.out.println(line);
		}
	}
	
	/**
	 * Get string descriptions of mapped locations for a set of records
	 * @param records The records
	 * @return Set of string representations of locations
	 */
	public static Set<String> getUniqueLocations(FeatureCollection<AvroSamRecord> records) {
		Set<String> rtrn = new TreeSet<String>();
		for(AvroSamRecord record : records) {
			rtrn.add(record.toUCSC());
		}
		return rtrn;
	}
	
	/**
	 * Get string representations of the mapped locations of all reads that share the same barcode as a record of interest
	 * @param record The record
	 * @param index Avro index
	 * @param Read IDs to exclude from results or null if not using
	 * @return The set of unique locations represented as strings
	 * @throws IOException
	 */
	public static Set<String> getUniqueLocationsOfInteractors(SAMRecord record, AvroSamStringIndex index, Collection<String> readIDsToExclude) throws IOException {
		Set<String> rtrn = new TreeSet<String>();
		String barcode = (String) record.getAttribute(BarcodedBamWriter.BARCODES_SAM_TAG);
		if(!numBarcodesOk(barcode)) {
			return rtrn;
		}
		Collection<String> ids = null;
		if(readIDsToExclude != null) {
			ids = new HashSet<String>();
			ids.addAll(readIDsToExclude);
		}
		FeatureCollection<AvroSamRecord> records = index.getAsAnnotationCollection(barcode, "qname", ids);
		rtrn.addAll(getUniqueLocations(records));
		return rtrn;
	}
	
	/**
	 * Get map of read location to the set of string representations of the mapped locations of all reads that share the same barcode
	 * @param index Avro index
	 * @param samReader SAM reader for bam file of read mappings
	 * @param chr Region chromosome
	 * @param start Region start
	 * @param end Region end
	 * @param excludeSelfMatches Exclude alternative mappings of the same read
	 * @return Map of read location to the set of string representations of the mapped locations of all reads that share the same barcode
	 * @throws IOException 
	 */
	public static Map<String, Set<String>> getInteractingLocations(AvroSamStringIndex index, SAMFileReader samReader, String chr, int start, int end, boolean excludeSelfMatches) throws IOException {
		Collection<String> readIDs = excludeSelfMatches ? getReadIDs(samReader, chr, start, end) : null;
		SAMRecordIterator iter = samReader.queryOverlapping(chr, start, end);
		Map<String, Set<String>> rtrn = new TreeMap<String, Set<String>>();
		while(iter.hasNext()) {
			SAMRecord record = iter.next();
			String location = record.getReferenceName() + ":" + record.getAlignmentStart() + "-" + record.getAlignmentEnd();
			Set<String> interactors = getUniqueLocationsOfInteractors(record, index, readIDs);
			if(rtrn.containsKey(location)) {
				rtrn.get(location).addAll(interactors);
			} else {
				rtrn.put(location, interactors);
			}
		}
		iter.close();
		return rtrn;
	}
	
	/**
	 * Get string representations of the mapped locations of all reads that share the same barcode as some read mapping to the region of interest
	 * @param index Avro index
	 * @param samReader SAM reader for bam file of read mappings
	 * @param chr Region chromosome
	 * @param start Region start
	 * @param end Region end
	 * @param excludeSelfMatches Exclude all reads with read ID matching a read in the region
	 * @return The set of unique locations represented as strings
	 * @throws IOException
	 */
	public static Set<String> getUniqueLocationsOfAllReadsWithBarcodesInRegion(AvroSamStringIndex index, SAMFileReader samReader, String chr, int start, int end, boolean excludeSelfMatches) throws IOException {
		Collection<String> barcodes = getBarcodes(samReader, chr, start, end);
		Collection<String> ids = null;
		if(excludeSelfMatches) {
			ids = getReadIDs(samReader, chr, start, end);
		}
		Set<String> rtrn = new TreeSet<String>();
		for(String barcode : barcodes) {
			FeatureCollection<AvroSamRecord> records = index.getAsAnnotationCollection(barcode, "qname", ids);
			rtrn.addAll(getUniqueLocations(records));
		}
		return rtrn;
	}
	
	/**
	 * Get number of reads per chr for all reads that share the same barcode as some read mapping to the region of interest
	 * @param index Avro index
	 * @param samReader SAM reader for bam file of read mappings
	 * @param chr Region chromosome
	 * @param start Region start
	 * @param end Region end
	 * @param excludeSelfMatches Exclude all reads with read ID matching a read in the region
	 * @return Map of reference name to number of reads
	 * @throws IOException
	 */
	public static Map<String, Integer> getChrCountsOfAllReadsWithBarcodesInRegion(AvroSamStringIndex index, SAMFileReader samReader, String chr, int start, int end, boolean excludeSelfMatches) throws IOException {
		Collection<String> barcodes = getBarcodes(samReader, chr, start, end);
		Collection<String> ids = null;
		if(excludeSelfMatches) {
			ids = getReadIDs(samReader, chr, start, end);
		}
		Map<String, Integer> rtrn = new TreeMap<String, Integer>();
		for(String barcode : barcodes) {
			FeatureCollection<AvroSamRecord> records = index.getAsAnnotationCollection(barcode, "qname", ids);
			CloseableIterator<AvroSamRecord> iter = records.sortedIterator();
			while(iter.hasNext()) {
				AvroSamRecord record = iter.next();
				String ref = record.getReferenceName();
				if(!rtrn.containsKey(ref)) {
					rtrn.put(ref, Integer.valueOf(0));
				}
				rtrn.put(ref, Integer.valueOf(rtrn.get(ref).intValue() + 1));
			}
			iter.close();
		}
		return rtrn;
	}
	
	
	/**
	 * Print number of reads per chr for all reads that share the same barcode as some read mapping to the region of interest
	 * @param index Avro index
	 * @param samReader SAM reader for bam file of read mappings
	 * @param chr Region chromosome
	 * @param start Region start
	 * @param end Region end
	 * @param excludeSelfMatches Exclude all reads with read ID matching a read in the region
	 * @throws IOException
	 */
	public static void printReferenceCountsOfAllReadsWithBarcodesInRegion(AvroSamStringIndex index, SAMFileReader samReader, String chr, int start, int end, boolean excludeSelfMatches) throws IOException {
		Map<String, Integer> counts = getChrCountsOfAllReadsWithBarcodesInRegion(index, samReader, chr, start, end, excludeSelfMatches);
		for(String c : counts.keySet()) {
			System.out.println(c + "\t" + counts.get(c));
		}
	}

	
	/**
	 * Print string representations of the mapped locations of all reads that share the same barcode as some read mapping to the region of interest
	 * @param index Avro index
	 * @param samReader SAM reader for bam file of read mappings
	 * @param chr Region chromosome
	 * @param start Region start
	 * @param end Region end
	 * @param excludeSelfMatches Exclude all reads with read ID matching a read in the region
	 * @throws IOException
	 */
	public static void printUniqueLocationsOfAllReadsWithBarcodesInRegion(AvroSamStringIndex index, SAMFileReader samReader, String chr, int start, int end, boolean excludeSelfMatches) throws IOException {
		Collection<String> locations = getUniqueLocationsOfAllReadsWithBarcodesInRegion(index, samReader, chr, start, end, excludeSelfMatches);
		for(String location : locations) {
			System.out.println(location);
		}
	}
	
	/**
	 * Print pairs of read location to the set of string representations of the mapped locations of all reads that share the same barcode
	 * @param index Avro index
	 * @param samReader SAM reader for bam file of read mappings
	 * @param chr Region chromosome
	 * @param start Region start
	 * @param end Region end
	 * @param excludeSelfMatches Exclude alternative mappings of the same read
	 * @throws IOException 
	 */
	public static void printInteractorPairsForRegion(AvroSamStringIndex index, SAMFileReader samReader, String chr, int start, int end, boolean excludeSelfMatches) throws IOException {
		Map<String, Set<String>> interactors = getInteractingLocations(index, samReader, chr, start, end, excludeSelfMatches);
		for(String key : interactors.keySet()) {
			for(String val : interactors.get(key)) {
				System.out.println(key + "\t" + val);
			}
		}
	}
	
	/**
	 * Print all reads sharing same barcodes
	 * @param index Avro index
	 * @param barcode Barcode to look for
	 * @throws IOException
	 */
	public static void printReadsWithBarcode(AvroSamStringIndex index, String barcode) throws IOException {
		FeatureCollection<AvroSamRecord> records = index.getAsAnnotationCollection(barcode);
		printRecords(records);
	}
	
	/**
	 * Print all reads with a barcode in a collection
	 * @param index Avro index
	 * @param barcodes Collection of barcodes to look for
	 * @throws IOException 
	 */
	public static void printReadsWithBarcodes(AvroSamStringIndex index, Collection<String> barcodes) throws IOException {
		for(String barcode : barcodes) {
			printReadsWithBarcode(index, barcode);
		}
	}
	
	/**
	 * Print all reads with the same barcode as any read overlapping a region
	 * @param index Avro index
	 * @param samReader SAM reader for bam file of read mappings
	 * @param chr Region chromosome
	 * @param start Region start
	 * @param end Region end
	 * @throws IOException
	 */
	public static void printReadsWithBarcodesInRegion(AvroSamStringIndex index, SAMFileReader samReader, String chr, int start, int end) throws IOException {
		Collection<String> barcodes = getBarcodes(samReader, chr, start, end);
		printReadsWithBarcodes(index, barcodes);
	}
	
	private static Map<String, Integer> readChrSizes(String file) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(file));
		StringParser s = new StringParser();
		Map<String, Integer> rtrn = new HashMap<String, Integer>();
		while(reader.ready()) {
			s.parse(reader.readLine());
			rtrn.put(s.asString(0), Integer.valueOf(s.asInt(1)));
		}
		reader.close();
		return rtrn;
	}
	
	public static void main(String[] args) throws IOException {
				
		CommandLineParser p = new CommandLineParser();
		p.addStringArg("-a", "Avro file", true);
		p.addStringArg("-b", "Print all reads with this barcode sequence", false);
		p.addStringArg("-s", "Avro schema file", true);
		p.addStringArg("-f", "Name of indexed field", true);
		p.addStringArg("-rc", "Print locations of all reads with barcodes matching some read mapped to region: chr", false);
		p.addIntArg("-rs", "Print locations of all reads with barcodes matching some read mapped to region: start", false, -1);
		p.addIntArg("-re", "Print locations of all reads with barcodes matching some read mapped to region: end", false, -1);
		p.addStringArg("-rbs", "Coordinate sorted bam file needed for query by region", false);
		p.addStringArg("-c", "Chromosome size file if querying entire chromosome. Leave out -rs and -re.", false, null);
		p.addIntArg("-mb", "Minimum number of barcodes to consider a fragment", false, MIN_NUM_BARCODES_PER_FRAGMENT);
		p.addBooleanArg("-es", "Exclude barcode matches if the read ID matches any read mapping to the query region", false, true);
		p.addBooleanArg("-pp", "Print pairs of locations that interact", false, false);
		p.addBooleanArg("-pl", "Print list of unique interacting locations", false, false);
		p.addBooleanArg("-pc", "Print counts of interacting reads mapped to each reference sequence", false, false);
		p.parse(args);
		String avroFile = p.getStringArg("-a");
		String barcode = p.getStringArg("-b");
		String schemaFile = p.getStringArg("-s");
		String indexedField = p.getStringArg("-f");
		String regionChr = p.getStringArg("-rc");
		int regionStart = p.getIntArg("-rs");
		int regionEnd = p.getIntArg("-re");
		String bam = p.getStringArg("-rbs");
		String sizeFile = p.getStringArg("-c");
		boolean excludeSelf = p.getBooleanArg("-es");
		boolean printPairs = p.getBooleanArg("-pp");
		boolean printLocations = p.getBooleanArg("-pl");
		boolean printRefCounts = p.getBooleanArg("-pc");
		MIN_NUM_BARCODES_PER_FRAGMENT = p.getIntArg("-mb");
		
		if(barcode != null && regionChr != null) {
			throw new IllegalArgumentException("Choose one: query by barcode or query by region");
		}
		
		AvroSamStringIndex index = new AvroSamStringIndex(avroFile, schemaFile, indexedField);

		if(barcode != null) {
			System.out.println();
			printReadsWithBarcode(index, barcode);
			System.out.println();
		}
		
		if(regionChr != null && regionStart < 0 && regionEnd < 0) {
			if(sizeFile == null) {
				throw new IllegalArgumentException("Must provide chr size file to query entire chromosome");
			}
			Map<String, Integer> sizes = readChrSizes(sizeFile);
			int size = sizes.get(regionChr).intValue();
			regionStart = 0;
			regionEnd = size;
		}

		if(regionChr != null && regionStart >= 0 && regionEnd >= 0) {
			if(bam == null) {
				throw new IllegalArgumentException("Must provide bam file for query by region");
			}
			if(!(regionStart >= 0 && regionEnd >= 0 && regionEnd >= regionStart)) {
				throw new IllegalArgumentException("Invalid region endpoints: " + regionStart + " " + regionEnd);
			}
			SAMFileReader samReader = new SAMFileReader(new File(bam));
			if(printPairs) {
				System.out.println();
				System.out.println("Pairs of interactors including one read in region " + regionChr + ":" + regionStart + "-" + regionEnd + ":");
				System.out.println("");
				printInteractorPairsForRegion(index, samReader, regionChr, regionStart, regionEnd, excludeSelf);
				System.out.println();
			} else if(printLocations) {
				System.out.println();
				System.out.println("Locations of reads with barcodes matching some read in region " + regionChr + ":" + regionStart + "-" + regionEnd + ":");
				System.out.println("");
				printUniqueLocationsOfAllReadsWithBarcodesInRegion(index, samReader, regionChr, regionStart, regionEnd, excludeSelf);
				System.out.println();
			} else if(printRefCounts) {
				System.out.println();
				System.out.println("Reference counts of reads with barcodes matching some read in region " + regionChr + ":" + regionStart + "-" + regionEnd + ":");
				System.out.println("");
				printReferenceCountsOfAllReadsWithBarcodesInRegion(index, samReader, regionChr, regionStart, regionEnd, excludeSelf);
				System.out.println();
			}
		}
		
		
		System.out.println("");
		System.out.println("All done.");
	}
	
	
}
