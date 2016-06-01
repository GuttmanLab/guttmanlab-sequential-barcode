package programs.query;

import guttmanlab.core.util.CommandLineParser;
import guttmanlab.core.util.StringParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
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

import contact.BarcodeSequence;
import programs.barcode.BarcodedBamWriter;
import guttmanlab.core.annotation.BEDFileRecord;
import guttmanlab.core.annotation.BlockedAnnotation;
import guttmanlab.core.annotation.Gene;
import guttmanlab.core.annotation.SingleInterval;
import guttmanlab.core.annotation.io.BEDFileIO;
import guttmanlab.core.annotationcollection.AnnotationCollection;
import guttmanlab.core.annotationcollection.FeatureCollection;
import guttmanlab.core.coordinatespace.CoordinateSpace;
import guttmanlab.core.serialize.AvroStringIndex;
import guttmanlab.core.serialize.sam.AvroSamRecord;
import guttmanlab.core.serialize.sam.AvroSamStringIndex;


/**
 * 
 * @author prussell
 *
 */
public final class Query3DBarcode {
	
	private static Logger logger = Logger.getLogger(Query3DBarcode.class.getName());
	private static int MIN_NUM_BARCODES_PER_FRAGMENT = 0;
	
	/**
	 * Prohibit instantiation
	 */
	private Query3DBarcode(){}
	
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
	private static Collection<String> getBarcodesOfReadsInRegion(SAMFileReader samReader, String chr, int start, int end) {
		Collection<String> rtrn = new HashSet<String>();
		SAMRecordIterator iter = samReader.queryOverlapping(chr, start, end);
		while(iter.hasNext()) {
			SAMRecord record = iter.next();
			if(!AvroSamRecord.mappingQualityIsOk(record)) {
				continue;
			}
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
	private static Collection<String> getReadIDs(SAMFileReader samReader, String chr, int start, int end) {
		Collection<String> rtrn = new HashSet<String>();
		SAMRecordIterator iter = samReader.queryOverlapping(chr, start, end);
		while(iter.hasNext()) {
			SAMRecord record = iter.next();
			if(!AvroSamRecord.mappingQualityIsOk(record)) {
				continue;
			}
			String id = record.getReadName();
			rtrn.add(id);
		}
		iter.close();
		return rtrn;
	}
	
	/**
	 * Print the records to std out and optionally to a bed file
	 * @param records Records to print
	 * @param outputBed Write in bed format to this location, or null if stdout
	 * @throws IOException 
	 */
	private static void printRecords(FeatureCollection<AvroSamRecord> records, String outputBed) throws IOException {
		if(outputBed != null) {
			FileWriter w = new FileWriter(outputBed);
			logger.info("Writing locations to bed file " + outputBed + "...");
			for(AvroSamRecord record : records) {
				w.write(record.toBED() + "\n");
			}
			logger.info("Done writing bed file.");
			w.close();
		} else {
			for(AvroSamRecord record : records) {
				String line = record.getName() + "\t";
				line += record.toUCSC() + "\t";
				line += record.getAttribute("tagXB") + "\t";
				System.out.println(line);
			}
		}
	}
	
	/**
	 * Get string descriptions of mapped locations for a set of records
	 * @param records The records
	 * @return Set of string representations of locations
	 */
	private static Set<String> getUniqueLocations(FeatureCollection<AvroSamRecord> records) {
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
	private static Set<String> getUniqueLocationsOfInteractors(SAMRecord record, AvroSamStringIndex index, Collection<String> readIDsToExclude) throws IOException {
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
		try {
			FeatureCollection<AvroSamRecord> records = index.getAsAnnotationCollection(barcode, "qname", ids);
			rtrn.addAll(getUniqueLocations(records));
		} catch(IllegalArgumentException e) {
			logger.warn("Skipping key: " + e.getMessage());
			return new TreeSet<String>();
		}
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
	private static Map<String, Set<String>> getInteractingLocations(AvroSamStringIndex index, SAMFileReader samReader, String chr, int start, int end, boolean excludeSelfMatches) throws IOException {
		Collection<String> regionReadIDs = excludeSelfMatches ? getReadIDs(samReader, chr, start, end) : null;
		SAMRecordIterator iter = samReader.queryOverlapping(chr, start, end);
		Map<String, Set<String>> rtrn = new TreeMap<String, Set<String>>();
		while(iter.hasNext()) {
			SAMRecord record = iter.next();
			if(!AvroSamRecord.mappingQualityIsOk(record)) {
				continue;
			}
			String location = record.getReferenceName() + ":" + record.getAlignmentStart() + "-" + record.getAlignmentEnd();
			Set<String> interactors = getUniqueLocationsOfInteractors(record, index, regionReadIDs);
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
	 * Get barcodes that are shared among multiple regions
	 * @param samReader SAM reader for bam file of read mappings
	 * @param regions The regions whose barcodes will be intersected
	 * @return The intersection of barcode sets for all the regions
	 */
	private static Collection<String> getIntersectionOfBarcodes(SAMFileReader samReader, Collection<SingleInterval> regions) {
		java.util.Iterator<SingleInterval> regionIter = regions.iterator();
		SingleInterval firstRegion = regionIter.next();
		Collection<String> rtrn = getBarcodesOfReadsInRegion(samReader, firstRegion.getReferenceName(), firstRegion.getReferenceStartPosition(), firstRegion.getReferenceEndPosition());
		while(regionIter.hasNext()) {
			SingleInterval region = regionIter.next();
			Collection<String> barcodes = getBarcodesOfReadsInRegion(samReader, region.getReferenceName(), region.getReferenceStartPosition(), region.getReferenceEndPosition());
			rtrn.retainAll(barcodes);
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
	 * @param excludeSelfMatches Exclude all reads with read ID matching a read in the region
	 * @return The set of unique locations represented as strings
	 * @throws IOException
	 */
	private static Set<String> getUniqueLocationsOfAllReadsWithBarcodesMatchingRegion(AvroSamStringIndex index, SAMFileReader samReader, 
			String chr, int start, int end, boolean excludeSelfMatches) throws IOException {
		Collection<String> barcodes = getBarcodesOfReadsInRegion(samReader, chr, start, end);
		Collection<String> ids = null;
		if(excludeSelfMatches) {
			ids = getReadIDs(samReader, chr, start, end);
		}
		Set<String> rtrn = new TreeSet<String>();
		for(String barcode : barcodes) {
			try {
				FeatureCollection<AvroSamRecord> records = index.getAsAnnotationCollection(barcode, "qname", ids);
				rtrn.addAll(getUniqueLocations(records));
			} catch(IllegalArgumentException e) {
				logger.warn("Skipping key: " + e.getMessage());
				continue;
			}
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
	 * @param normalizeByQueryRegionRPKM Normalize all returned counts by RPKM of the query region
	 * @param totalMappedReads Total number of mapped reads for RPKM calculation, or pass a nonpositive number to calculate on the fly
	 * @return Map of reference name to number of reads
	 * @throws IOException
	 */
	private static Map<String, Double> getChrCountsOfAllReadsWithBarcodesMatchingRegion(AvroSamStringIndex index, SAMFileReader samReader, String chr, int start, int end, boolean excludeSelfMatches, boolean normalizeByQueryRegionRPKM, long totalMappedReads) throws IOException {
		Collection<String> barcodes = getBarcodesOfReadsInRegion(samReader, chr, start, end);
		Collection<String> ids = null;
		if(excludeSelfMatches) {
			ids = getReadIDs(samReader, chr, start, end);
		}
		Map<String, Double> rtrn = new TreeMap<String, Double>();
		for(String barcode : barcodes) {
			try {
				FeatureCollection<AvroSamRecord> records = index.getAsAnnotationCollection(barcode, "qname", ids);
				CloseableIterator<AvroSamRecord> iter = records.sortedIterator();
				while(iter.hasNext()) {
					AvroSamRecord record = iter.next();
					String ref = record.getReferenceName();
					if(!rtrn.containsKey(ref)) {
						rtrn.put(ref, Double.valueOf(0));
					}
					rtrn.put(ref, Double.valueOf(rtrn.get(ref).intValue() + 1));
				}
				iter.close();
			} catch(IllegalArgumentException e) {
				logger.warn("Skipping key: " + e.getMessage());
				continue;
			}
		}
		if(normalizeByQueryRegionRPKM) {
			if(totalMappedReads < 0) {
				logger.info("No total read count provided for RPKM calculation. Calculating from bam file...");
				SAMRecordIterator samRecordIter = samReader.iterator();
				totalMappedReads = 0;
				while(samRecordIter.hasNext()) {
					SAMRecord record = samRecordIter.next();
					if(!record.getReadUnmappedFlag()) {
						totalMappedReads++;
					}
				}
				logger.info("There are " + totalMappedReads + " mapped reads.");
			}
			double regionKb = (double) (end - start + 1) / 1000;
			SAMRecordIterator iter = samReader.queryOverlapping(chr, start, end);
			double regionCount = 0;
			while(iter.hasNext()) {
				iter.next();
				regionCount++;
			}
			double millionsOfReads = (double) totalMappedReads / 1000000;
			double rpkm = (regionCount / regionKb) / millionsOfReads;
			logger.info("RPKM of " + chr + ":" + start + "-" + end + " is " + rpkm + ".");
			for(String ref : rtrn.keySet()) {
				rtrn.put(ref, Double.valueOf(rtrn.get(ref).doubleValue() / rpkm));
			}			
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
	 * @param normalizeByQueryRegionRPKM Normalize all returned counts by RPKM of the query region
	 * @param totalMappedReads Total number of mapped reads for RPKM calculation, or pass a nonpositive number to calculate on the fly
	 * @throws IOException
	 */
	private static void printReferenceCountsOfAllReadsWithBarcodesInRegion(AvroSamStringIndex index, SAMFileReader samReader, String chr, int start, int end, boolean excludeSelfMatches, boolean normalizeByQueryRegionRPKM, long totalMappedReads) throws IOException {
		Map<String, Double> counts = getChrCountsOfAllReadsWithBarcodesMatchingRegion(index, samReader, chr, start, end, excludeSelfMatches, normalizeByQueryRegionRPKM, totalMappedReads);
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
	 * @param outputBed Write in bed format to this location, or null if stdout
	 * @throws IOException
	 */
	private static void printUniqueLocationsOfAllReadsWithBarcodesInRegion(AvroSamStringIndex index, SAMFileReader samReader, String chr, int start, int end, boolean excludeSelfMatches, String outputBed) throws IOException {
		Collection<String> locations = getUniqueLocationsOfAllReadsWithBarcodesMatchingRegion(index, samReader, chr, start, end, excludeSelfMatches);
		if(outputBed != null) {
			FileWriter w = new FileWriter(outputBed);
			logger.info("Writing locations to bed file " + outputBed + "...");
			for(String location : locations) {
				StringParser s = new StringParser();
				s.parse(location, ":");
				String ref = s.asString(0);
				StringParser s2 = new StringParser();
				s2.parse(s.asString(1), "-");
				String st = s2.asString(0);
				String en = s2.asString(1);
				w.write(ref + "\t" + st + "\t" + en + "\t" + location + "\n");
			}
			logger.info("Done writing bed file.");
			w.close();
		} else {
			for(String location : locations) {
				System.out.println(location);
			}
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
	 * @param outputBed Write in bed format to this location, or null if stdout
	 * @throws IOException 
	 */
	private static void printInteractorPairsForRegion(AvroSamStringIndex index, SAMFileReader samReader, String chr, int start, int end, boolean excludeSelfMatches, String outputBed) throws IOException {
		Map<String, Set<String>> interactors = getInteractingLocations(index, samReader, chr, start, end, excludeSelfMatches);
		if(outputBed != null) {
			FileWriter w = new FileWriter(outputBed);
			logger.info("Writing locations to bed file " + outputBed + "...");
			for(String key : interactors.keySet()) {
				for(String val : interactors.get(key)) {
					StringParser s = new StringParser();
					s.parse(val, ":");
					String ref = s.asString(0);
					StringParser s2 = new StringParser();
					s2.parse(s.asString(1), "-");
					String st = s2.asString(0);
					String en = s2.asString(1);
					w.write(ref + "\t" + st + "\t" + en + "\t" + key + "_" + val + "\n");
				}
			}
			logger.info("Done writing bed file.");
			w.close();
		} else {
			for(String key : interactors.keySet()) {
				for(String val : interactors.get(key)) {
					System.out.println(key + "\t" + val);
				}
			}
		}
	}
	
	/**
	 * Print all reads sharing same barcodes
	 * @param index Avro index
	 * @param barcode Barcode to look for
	 * @param outputBed Write in bed format to this location, or null if stdout
	 * @throws IOException
	 */
	private static void printReadsWithBarcode(AvroSamStringIndex index, String barcode, String outputBed) throws IOException {
		try {
			FeatureCollection<AvroSamRecord> records = index.getAsAnnotationCollection(barcode);
			printRecords(records, outputBed);
		} catch(IllegalArgumentException e) {
			logger.warn("Skipping key: " + e.getMessage());
		}
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
		p.addStringArg("-c", "Chromosome size file (to exclude regions with -be, or for entire chromosome with -rc; leave out -rs and -re)", false, null);
		p.addIntArg("-mb", "Minimum number of barcodes to consider a fragment", false, MIN_NUM_BARCODES_PER_FRAGMENT);
		p.addBooleanArg("-es", "Exclude barcode matches if the read ID matches any read mapping to the query region", false, true);
		p.addBooleanArg("-pp", "Print pairs of locations that interact", false, false);
		p.addBooleanArg("-pl", "Print list of unique interacting locations", false, false);
		p.addBooleanArg("-pc", "Print counts of interacting reads mapped to each reference sequence", false, false);
		p.addStringArg("-be", "Bed file of regions to exclude from matches (remove matches that overlap this annotation", false, null);
		p.addStringArg("-cbi", "Print barcodes that are present on all of this comma-separated list of chromosomes", false, null);
		p.addLongArg("-mr", "Max records to get for each barcode. If more than this amount, don't get any", false, AvroStringIndex.MAX_RECORDS_TO_GET);
		p.addIntArg("-mmq", "Min mapping quality to apply to all sam records", false, AvroSamRecord.MIN_MAPPING_QUALITY);
		p.addStringArg("-ob", "Output bed file, omit if printing to standard out", false, null);
		p.addBooleanArg("-ncr", "For -pc option, normalize counts by RPKM of the query region", false, false);
		p.addLongArg("-trc", "Total read count for RPKM calculation in -ncr option. If omitted, calculate on the fly.", false, -1);
		p.parse(args);
		AvroSamRecord.MIN_MAPPING_QUALITY = p.getIntArg("-mmq");
		if(AvroSamRecord.MIN_MAPPING_QUALITY > 0) {
			// If there is a min mapq, also make sure mapq is not 255 (means unavailable)
			AvroSamRecord.MAX_MAPPING_QUALITY = 254;
		}
		AvroStringIndex.MAX_RECORDS_TO_GET = p.getLongArg("-mr");
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
		String excludeBed = p.getStringArg("-be");
		String chrBarcodeIntersection = p.getStringArg("-cbi");
		String outputBed = p.getStringArg("-ob");
		boolean normalizeByQueryRegionRPKM = p.getBooleanArg("-ncr");
		long totalReadCount = p.getLongArg("-trc");
		
		if(chrBarcodeIntersection != null) {
			StringParser s = new StringParser();
			s.parse(chrBarcodeIntersection, ",");
			String[] chrs = s.getStringArray();
			if(sizeFile == null) {
				throw new IllegalArgumentException("Must provide chr size file to query entire chromosome");
			}
			if(bam == null) {
				throw new IllegalArgumentException("Must provide bam file for query by region");
			}
			SAMFileReader samReader = new SAMFileReader(new File(bam));
			Map<String, Integer> sizes = readChrSizes(sizeFile);
			Collection<SingleInterval> intervals = new ArrayList<SingleInterval>();
			for(String chr : chrs) {
				int size = sizes.get(chr).intValue();
				int start = 0;
				int end = size;
				SingleInterval interval = new SingleInterval(chr, start, end);
				intervals.add(interval);
			}
			Collection<String> sharedBarcodes = getIntersectionOfBarcodes(samReader, intervals);
			System.out.println();
			if(sharedBarcodes.isEmpty()) {
				System.out.println("No shared barcodes");
			} else {
				for(String sharedBarcode : sharedBarcodes) {
					System.out.println(sharedBarcode.toString());
				}
			}
			System.out.println();
			return;
		}
		
		if(barcode != null && regionChr != null) {
			throw new IllegalArgumentException("Choose one: query by barcode or query by region");
		}
		
		AnnotationCollection<? extends BlockedAnnotation> regionsToExclude = null;
		
		if(excludeBed != null) {
			if(sizeFile == null) {
				throw new IllegalArgumentException("If excluding regions with -be, must provide chromosome size file with -c.");
			}
			regionsToExclude = BEDFileIO.loadFromFile(excludeBed, new CoordinateSpace(sizeFile));
		}
		
		AvroSamStringIndex index = new AvroSamStringIndex(avroFile, schemaFile, indexedField, regionsToExclude);

		if(barcode != null) {
			System.out.println();
			printReadsWithBarcode(index, barcode, outputBed);
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
				printInteractorPairsForRegion(index, samReader, regionChr, regionStart, regionEnd, excludeSelf, outputBed);
				System.out.println();
			} else if(printLocations) {
				System.out.println();
				System.out.println("Locations of reads with barcodes matching some read in region " + regionChr + ":" + regionStart + "-" + regionEnd + ":");
				System.out.println("");
				printUniqueLocationsOfAllReadsWithBarcodesInRegion(index, samReader, regionChr, regionStart, regionEnd, excludeSelf, outputBed);
				System.out.println();
			} else if(printRefCounts) {
				System.out.println();
				String message = "Reference counts of reads with barcodes matching some read in region " + regionChr + ":" + regionStart + "-" + regionEnd;
				if(normalizeByQueryRegionRPKM) {
					message += " (normalized by region RPKM)";
				}
				message += ":";
				System.out.println(message);
				System.out.println("");
				printReferenceCountsOfAllReadsWithBarcodesInRegion(index, samReader, regionChr, regionStart, regionEnd, excludeSelf, normalizeByQueryRegionRPKM, totalReadCount);
				System.out.println();
			}
		}
		
		
		System.out.println("");
		System.out.println("All done.");
	}
	
	
}
