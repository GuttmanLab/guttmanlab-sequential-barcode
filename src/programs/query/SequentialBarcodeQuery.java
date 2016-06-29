package programs.query;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

import util.BinCounts;
import util.CustomSamTag;
import util.Filters;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import net.sf.samtools.util.CloseableIterator;
import guttmanlab.core.annotation.BEDFileRecord;
import guttmanlab.core.annotation.BlockedAnnotation;
import guttmanlab.core.annotation.io.BEDFileIO;
import guttmanlab.core.coordinatespace.CoordinateSpace;
import guttmanlab.core.pipeline.util.BamUtils;
import guttmanlab.core.serialize.sam.AvroSamRecord;
import guttmanlab.core.serialize.sam.AvroSamStringIndex;
import guttmanlab.core.util.CommandLineParser;
import guttmanlab.core.util.StringParser;

/**
 * Query an avro database for interactors
 * @author prussell
 *
 */
public class SequentialBarcodeQuery {
	
	public static final String BARCODE_FIELD_NAME = "tagXB";
	public static final String TRANSCRIPT_OVERLAPPER_FIELD_NAME = "tagXT";
	
	private SAMFileReader samReader; // Reader for bam file with barcode and gene overlap information
	private AvroSamStringIndex avroIndex;
	private Map<String, BlockedAnnotation> featuresByName;
	private Collection<Predicate<SAMRecord>> readFilters;
	private Collection<Predicate<AvroSamRecord>> avroFilters;
	
	private static final Logger logger = Logger.getLogger(SequentialBarcodeQuery.class.getName());
	
	/**
	 * Builder for objects of this class
	 * Must set all fields
	 * @author prussell
	 *
	 */
	public static class QueryEnvironmentBuilder {
		
		private SequentialBarcodeQuery query;
		
		public QueryEnvironmentBuilder() {query = new SequentialBarcodeQuery();};
		
		/**
		 * @param samReader Reader for bam file with barcode and gene overlap tags
		 */
		public void setSamReader(SAMFileReader samReader) {query.samReader = samReader;}
		
		/**
		 * @param barcodedBam Bam file with barcode and gene overlap tags
		 */
		public void setSamReader(File barcodedBam) {query.samReader = new SAMFileReader(barcodedBam);}
		
		/**
		 * @param avroIndex Avro index
		 */
		public void setAvroIndex(AvroSamStringIndex avroIndex) {query.avroIndex = avroIndex;}
		
		/**
		 * @param avroFile Avro database file
		 * @param schemaFile Avro schema file
		 */
		public void setAvroIndex(File avroFile, File schemaFile) {
			query.avroIndex = new AvroSamStringIndex(avroFile.getAbsolutePath(), schemaFile.getAbsolutePath(), BARCODE_FIELD_NAME);
		}
		
		/**
		 * @param featuresByName Map of feature name to feature for queries
		 */
		public void setFeatures(Map<String, BlockedAnnotation> featuresByName) {query.featuresByName = featuresByName;}
		
		/**
		 * @param featureBed Bed file of genes/features for queries
		 * @param genome Name of genome assembly e.g. "mm10"
		 */
		public void setFeatures(File featureBed, String genome) {query.featuresByName = featuresByName(featureBed, genome);}
		
		/**
		 * @param readFilters Read filters for barcoded bam file
		 */
		public void setReadFilters(Collection<Predicate<SAMRecord>> readFilters) {query.readFilters = readFilters;}
		
		/**
		 * @param avroFilters Filters for avro records from database
		 */
		public void setAvroFilters(Collection<Predicate<AvroSamRecord>> avroFilters) {query.avroFilters = avroFilters;}
		
		/**
		 * Get the constructed query environment object
		 * @return The query environment
		 */
		public SequentialBarcodeQuery get() {
			if(query.samReader == null) throw new IllegalStateException("Provide SAM reader");
			if(query.avroIndex == null) throw new IllegalStateException("Provide avro index");
			if(query.featuresByName == null) throw new IllegalStateException("Provide features");
			if(query.readFilters == null) throw new IllegalStateException("Provide read filters");
			if(query.avroFilters == null) throw new IllegalStateException("Provide avro record filters");
			return query;
		}
		
	}
	
	// Prevent instantiation
	private SequentialBarcodeQuery() {}
	
	/*
	 * Create map of feature name to feature from a bed file
	 */
	private static Map<String, BlockedAnnotation> featuresByName(File bed, String genome) {
		logger.info("Loading features from " + bed.getAbsolutePath());
		Map<String, BlockedAnnotation> rtrn = new HashMap<String, BlockedAnnotation>();
		try {
			CloseableIterator<BEDFileRecord> features = BEDFileIO.loadFromFile(bed, CoordinateSpace.forGenome(genome)).sortedIterator();
			while(features.hasNext()) {
				BEDFileRecord feature = features.next();
				rtrn.put(feature.getName(), feature);
			}
			return rtrn;
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		logger.info("Loaded " + rtrn.size() + " features.");
		return rtrn;
	}
	
	
	
	/**
	 * Get all records in database with specified barcode string
	 * @param barcodeSequence Barcode string
	 * @return All records with this barcode string
	 */
	private Stream<AvroSamRecord> getRecordsWithBarcodeSequence(String barcodeSequence) {
		return avroIndex.get(barcodeSequence)
				.stream()
				.filter(x -> Filters.passesAll(x, avroFilters));
	}
	
	/**
	 * Get all records in database with one of the specified barcode string
	 * @param barcodeSequences Barcode strings
	 * @return All records with one of these barcode strings
	 */
	private Stream<AvroSamRecord> getRecordsWithBarcodeSequenceIn(Collection<String> barcodeSequences) {
		return barcodeSequences.stream().flatMap(barcode -> getRecordsWithBarcodeSequence(barcode));
	}
	
	/**
	 * Check if a SAM record has the custom overlap tag indicating that it overlaps the feature
	 * @param featureName The feature name
	 * @return True if the record has the tag including the feature name
	 */
	private static boolean hasOverlapTag(SAMRecord record, String featureName) {
		String tag = record.getStringAttribute(CustomSamTag.TRANSCRIPT_OVERLAPPERS);
		if(tag == null) return false;
		if(tag.equals("")) return false;
		return CustomSamTag.transcriptIDs(tag).contains(featureName);
	}
	
	/**
	 * A genomic interval with ref, start, end
	 * @author prussell
	 *
	 */
	private static class Interval {
		
		private String ref;
		private int start;
		private int end;
		
		/**
		 * @param ucsc Interval in UCSC format chr:start-end
		 */
		public Interval(String ucsc) {
			StringParser s = new StringParser();
			s.parse(ucsc, ":");
			if(s.getFieldCount() != 2) throw new IllegalArgumentException("Invalid UCSC format: " + ucsc);
			ref = s.asString(0);
			String coord = s.asString(1);
			s.parse(coord, "-");
			if(s.getFieldCount() != 2) throw new IllegalArgumentException("Invalid UCSC format: " + ucsc);
			start = s.asInt(0);
			end = s.asInt(1);
		}
		
	}
	
	private boolean isFeatureName(String s) {
		if(featuresByName.containsKey(s)) return true;
		else {
			try {
				@SuppressWarnings("unused")
				Interval in = new Interval(s);
			} catch(IllegalArgumentException e) {
				throw new IllegalArgumentException("Not a valid feature ID or UCSC-formatted interval: " + s);
			}
			return false;
		}
	}
	
	/**
	 * Get reads overlapping the region
	 * @param region Feature ID or interval in UCSC format. If a feature, 
	 * require reads to report overlap with this feature in their XT tag
	 * @return Reads that overlap the region
	 */
	private Collection<SAMRecord> getOverlappers(String region) {
		Collection<SAMRecord> rtrn = new ArrayList<SAMRecord>();
		
		if(isFeatureName(region)) {
			BlockedAnnotation feature = featuresByName.get(region);
			SAMRecordIterator iter = samReader.query(feature.getReferenceName(), feature.getReferenceStartPosition(), feature.getReferenceEndPosition(), false);
			iter.forEachRemaining(record -> {if(hasOverlapTag(record, region) && Filters.passesAll(record, readFilters)) rtrn.add(record);});
			iter.close();
			return rtrn;
		}
		
		else {
			Interval interval = new Interval(region);
			SAMRecordIterator iter = samReader.query(interval.ref, interval.start, interval.end, false);
			iter.forEachRemaining(record -> {if(Filters.passesAll(record, readFilters)) rtrn.add(record);});
			iter.close();
			return rtrn;
		}
		
	}
	
	/**
	 * Get set of barcode sequences for reads in the collection
	 * @param samRecords Collection of SAM records
	 * @return Set of barcode sequences encoded in custom tag for all the records
	 */
	private static Set<String> getAllBarcodes(Collection<SAMRecord> samRecords) {
		Set<String> rtrn = new TreeSet<String>();
		samRecords
			.iterator()
			.forEachRemaining(
				record -> rtrn.add(
						record.getStringAttribute(CustomSamTag.BARCODE_SEQUENCE)));
		return rtrn;
	}
	
	/**
	 * Get the names of transcripts listed in the transcript overlapper tag of records in the stream
	 * @param recordStream Stream of AvroSamRecords
	 * @return Set of transcript IDs listed in the transcript overlapper tag of any of the records in the stream
	 */
	private static Set<String> getTranscriptOverlapperNames(Stream<AvroSamRecord> recordStream) {
		return recordStream
				.map(record -> record.getStringAttribute(TRANSCRIPT_OVERLAPPER_FIELD_NAME))
				.filter(opt -> opt.isPresent())
				.map(opt -> opt.get())
				.flatMap(str -> Arrays.asList(str.replaceAll("]", "").split("\\[")).stream())
				.collect(Collectors.toSet());
	}
	
	/**
	 * Get records that share a barcode with a record overlapping the given region
	 * @param region Query feature ID or interval in UCSC format
	 * @return Stream of records that share a barcode sequence with a record overlapping the given region
	 */
	public Stream<AvroSamRecord> getInteractingRecords(String region) {
		logger.info("Getting interacting records for region " + region);
		return getRecordsWithBarcodeSequenceIn(getAllBarcodes(getOverlappers(region)));
	}
	
	/**
	 * Get the names of features that share a barcode sequence with a record overlapping the given region
	 * @param region Query feature ID or interval in UCSC format
	 * @return Names of features that share a barcode sequence with a record overlapping the given region
	 */
	public Set<String> getInteractingFeatureNames(String region) {
		logger.info("Getting interacting feature names for feature " + region);
		return getTranscriptOverlapperNames(getInteractingRecords(region));
				
	}
	
	@SuppressWarnings("unused")
	private static void print(Collection<String> strings) {
		System.out.println(strings.stream().collect(Collectors.joining("\n")));
	}
	
	private static void write(Collection<String> strings, File out) {
		try {
			FileWriter w = new FileWriter(out);
			w.write(strings.stream().collect(Collectors.joining("\n")) + "\n");
			w.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	
	public static void main(String[] args) {
		
		CommandLineParser p = new CommandLineParser();
		p.addStringArg("-ad", "Avro database file", true);
		p.addStringArg("-as", "Avro schema file", true);
		p.addStringArg("-fb", "Bed file of features that match the XT tag in bam file", true);
		p.addStringArg("-g", "Name of genome e.g. mm10", true);
		p.addStringArg("-bb", "Coordinate-sorted bam file with custom tags: XA (if using), XB (required), XT (required)", true);
		p.addStringArg("-f", "Query region (feature ID or interval in UCSC format)", true);
		p.addStringArg("-op", "Output prefix", true);
		p.addIntArg("-bs", "Bin size for count table", false, 1000000);
		p.parse(args);
		File avroFile = new File(p.getStringArg("-ad"));
		File schemaFile = new File(p.getStringArg("-as"));
		File featureBed = new File(p.getStringArg("-fb"));
		String genome = p.getStringArg("-g");
		File bamFile = new File(p.getStringArg("-bb"));
		String queryRegion = p.getStringArg("-f");
		String outPrefix = p.getStringArg("-op");
		int binSize = p.getIntArg("-bs");
		
		// Create file names
		String sam = outPrefix + ".interactions.sam";
		String table = outPrefix + ".interactions.features.txt";
		String bam = sam.replaceAll(".sam", ".bam");
		String sortedBam = bam.replaceAll(".bam", ".sorted.bam");

		// Build the query object
		QueryEnvironmentBuilder builder = new QueryEnvironmentBuilder();
		builder.setAvroFilters(Filters.defaultAvroFilters());
		builder.setAvroIndex(avroFile, schemaFile);
		builder.setFeatures(featureBed, genome);
		builder.setReadFilters(Filters.defaultSamFilters());
		builder.setSamReader(bamFile);
		SequentialBarcodeQuery query = builder.get();

		// Write names of interacting features
		System.out.println("\n");
		logger.info("Writing interacting transcripts to " + table);
		write(query.getInteractingFeatureNames(queryRegion), new File(table));
			
		// Write SAM file of interactors
		System.out.println("");
		logger.info("Writing interacting records to " + sam);
		SAMFileHeader header = new SAMFileHeader();
		header.setTextHeader(query.samReader.getFileHeader().getTextHeader().replaceAll("SO:coordinate", "SO:unsorted"));
		header.setSortOrder(SAMFileHeader.SortOrder.unsorted);
		AvroSamRecord.writeToSAM(query.getInteractingRecords(queryRegion), header, new File(sam));
		System.out.println("");
		BamUtils.samToBam(new File(sam), new File(bam));
		System.out.println("");
		BamUtils.sortBam(new File(bam), new File(sortedBam));
		System.out.println("");
		BamUtils.indexBam(new File(sortedBam));
		
		// Write table of bin counts
		System.out.println("");
		String binFile = outPrefix + ".binCounts.txt";
		logger.info("Writing bin counts to " + binFile);
		BinCounts.writeCounts(new File(sortedBam), CoordinateSpace.forGenome(genome), binSize, new File(binFile));
		
		logger.info("");
		logger.info("All done.");
		
	}
	
	
	
}

