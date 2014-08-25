package sequentialbarcode.avro;

import general.CommandLineParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import net.sf.samtools.Cigar;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.TextCigarCodec;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import broad.core.parser.StringParser;

import sequentialbarcode.BarcodeSequence;
import sequentialbarcode.BarcodedBamWriter;
import serialize.AvroIndex;
import serialize.AvroStringIndex;


/**
 * 
 * @author prussell
 *
 */
public class Query3DBarcode {
	
	@SuppressWarnings("unused")
	private static Logger logger = Logger.getLogger(Query3DBarcode.class.getName());
	private static int MIN_NUM_BARCODES_PER_FRAGMENT = 0;
		
	/**
	 * Get the collection of barcode attributes for all reads overlapping a region
	 * @param data samReader SAM reader for bam file of read mappings
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
			BarcodeSequence barcodes = BarcodeSequence.fromSamAttributeString(barcodeString);
			if(barcodes.getNumBarcodes() >= MIN_NUM_BARCODES_PER_FRAGMENT) {
				rtrn.add(barcodeString);
			}
		}
		return rtrn;
	}
	
	/**
	 * Print the records to std out
	 * @param records Records to print
	 */
	private static void printRecords(List<GenericRecord> records) {
		for(GenericRecord record : records) {
			String line = record.get("qname") + "\t";
			line += getPositionAsUCSC(record) + "\t";
			line += record.get("tagXB") + "\t";
			System.out.println(line);
		}
	}
	
	/**
	 * Get string descriptions of mapped locations for a set of records
	 * @param records The records
	 * @return Set of string representations of locations
	 */
	public static Set<String> getUniqueLocations(List<GenericRecord> records) {
		Set<String> rtrn = new TreeSet<String>();
		for(GenericRecord record : records) {
			rtrn.add(getPositionAsUCSC(record));
		}
		return rtrn;
	}
	
	/**
	 * Get string representations of the mapped locations of all reads that share the same barcode as some read mapping to the region of interest
	 * @param index Avro index
	 * @param samReader SAM reader for bam file of read mappings
	 * @param chr Region chromosome
	 * @param start Region start
	 * @param end Region end
	 * @return The set of unique locations represented as strings
	 * @throws IOException
	 */
	public static Set<String> getUniqueLocationsOfAllReadsWithBarcodesInRegion(AvroIndex<String> index, SAMFileReader samReader, String chr, int start, int end) throws IOException {
		Collection<String> barcodes = getBarcodes(samReader, chr, start, end);
		Set<String> rtrn = new TreeSet<String>();
		for(String barcode : barcodes) {
			List<GenericRecord> records = index.get(barcode);
			rtrn.addAll(getUniqueLocations(records));
		}
		return rtrn;
	}
	
	/**
	 * Print string representations of the mapped locations of all reads that share the same barcode as some read mapping to the region of interest
	 * @param index Avro index
	 * @param samReader SAM reader for bam file of read mappings
	 * @param chr Region chromosome
	 * @param start Region start
	 * @param end Region end
	 * @throws IOException
	 */
	public static void printUniqueLocationsOfAllReadsWithBarcodesInRegion(AvroIndex<String> index, SAMFileReader samReader, String chr, int start, int end) throws IOException {
		Collection<String> locations = getUniqueLocationsOfAllReadsWithBarcodesInRegion(index, samReader, chr, start, end);
		for(String location : locations) {
			System.out.println(location);
		}
	}
	
	/**
	 * Print all reads sharing same barcodes
	 * @param index Avro index
	 * @param barcode Barcode to look for
	 * @throws IOException
	 */
	public static void printReadsWithBarcode(AvroIndex<String> index, String barcode) throws IOException {
		List<GenericRecord> records = index.get(barcode);
		printRecords(records);
	}
	
	/**
	 * Print all reads with a barcode in a collection
	 * @param index Avro index
	 * @param barcodes Collection of barcodes to look for
	 * @throws IOException 
	 */
	public static void printReadsWithBarcodes(AvroIndex<String> index, Collection<String> barcodes) throws IOException {
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
	public static void printReadsWithBarcodesInRegion(AvroIndex<String> index, SAMFileReader samReader, String chr, int start, int end) throws IOException {
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
		MIN_NUM_BARCODES_PER_FRAGMENT = p.getIntArg("-mb");
		
		if(barcode != null && regionChr != null) {
			throw new IllegalArgumentException("Choose one: query by barcode or query by region");
		}
		
		AvroIndex<String> index = new AvroStringIndex(avroFile, schemaFile, indexedField);

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
			System.out.println();
			System.out.println("Locations of reads with barcodes matching some read in region " + regionChr + ":" + regionStart + "-" + regionEnd + ":");
			System.out.println("");
			printUniqueLocationsOfAllReadsWithBarcodesInRegion(index, samReader, regionChr, regionStart, regionEnd);
			System.out.println();
		}
		
		
		System.out.println("");
		System.out.println("All done.");
	}
	
	/**
	 * @param samRecord SAM record
	 * @return A string representation of mapped location
	 */
	public static String getPositionAsUCSC(GenericRecord samRecord) {
		String cigarString = samRecord.get("cigar").toString();
		TextCigarCodec codec = new TextCigarCodec();
		Cigar cigar = codec.decode(cigarString);
		int length = cigar.getReferenceLength();
		int start = (int) samRecord.get("pos");
		int end = start + length;
		return samRecord.get("rname") + ":" + start + "-" + end;
	}
	
}
