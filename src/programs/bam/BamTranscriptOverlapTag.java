package programs.bam;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import util.CustomSamTag;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMFileWriterFactory;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import guttmanlab.core.annotation.Annotation;
import guttmanlab.core.annotation.BlockedAnnotation;
import guttmanlab.core.annotation.SAMFragment;
import guttmanlab.core.annotation.io.BEDFileIO;
import guttmanlab.core.annotationcollection.FeatureCollection;
import guttmanlab.core.coordinatespace.CoordinateSpace;
import guttmanlab.core.util.CommandLineParser;

/**
 * Add a tag to SAM records that specifies which features they overlap
 * @author prussell
 *
 */
public class BamTranscriptOverlapTag {
	
	private static Logger logger = Logger.getLogger(BamTranscriptOverlapTag.class.getName());
	
	/**
	 * @param featureBed Bed file of features
	 * @param genome Name of assembly e.g. mm10
	 */
	private BamTranscriptOverlapTag(String featureBed, CoordinateSpace genome) {
		try {
			transcripts = (FeatureCollection<? extends BlockedAnnotation>) BEDFileIO.loadFromFile(featureBed, genome);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	/**
	 * The features to check for overlap with SAM records
	 */
	private FeatureCollection<? extends BlockedAnnotation> transcripts;
	
	/**
	 * Get all the features overlapping a record
	 * @param record The SAM record
	 * @return The collection of overlappers
	 */
	private FeatureCollection<? extends BlockedAnnotation> overlappers(SAMRecord record) {
		Annotation asAnnot = new SAMFragment(record);
		return transcripts.overlappers(asAnnot);
	}
	
	/**
	 * Convert a collection of features to the string tag value
	 * @param features The features
	 * @return The value of the tag
	 */
	private static String toTagValue(FeatureCollection<? extends Annotation> features) {
		return features.stream()
				.map(feature -> "[" + feature.getName() + "]")
				.sorted()
				.collect(Collectors.joining());
	}
	
	/**
	 * Set the overlappers tag of a SAM record by identifying overlappers
	 * @param record The record
	 */
	private void setTag(SAMRecord record) {
		FeatureCollection<? extends BlockedAnnotation> overlappers = overlappers(record);
		if(!overlappers.isEmpty()) record.setAttribute(CustomSamTag.TRANSCRIPT_OVERLAPPERS, toTagValue(overlappers));
	}
	
	/**
	 * Add the tag to every record in a BAM file
	 * @param inputBam Input BAM file
	 * @param outputBam Output BAM file including the tag
	 */
	private void addTag(File inputBam, File outputBam) {
		int numDone = 0;
		SAMFileReader reader = new SAMFileReader(inputBam);
		SAMFileWriter writer = new SAMFileWriterFactory().makeBAMWriter(reader.getFileHeader(), true, outputBam);
		SAMRecordIterator iter = reader.iterator();
		while(iter.hasNext()) {
			numDone++;
			if(numDone % 100000 == 0) logger.info("Finished " + numDone + " records");
			SAMRecord record = iter.next();
			setTag(record);
			writer.addAlignment(record);
		}
		reader.close();
		writer.close();
	}
	
	public static void main(String[] args) {
		
		CommandLineParser p = new CommandLineParser();
		p.addStringArg("-i", "Input bam file", true);
		p.addStringArg("-o", "Output bam file", true);
		p.addStringArg("-f", "Feature bed file", true);
		p.addStringArg("-g", "Genome name e.g. mm10. Provide either this or coordinate space file.", false);
		p.addStringArg("-cs", "Coordinate space file. Provide either this or genome name.", false);
		p.parse(args);
		String genome = p.getStringArg("-g");
		String chrFile = p.getStringArg("-cs");
		CoordinateSpace cs = null;
		if(genome != null) {
			if(chrFile != null) throw new IllegalArgumentException("Provide genome name or coordinate space file, not both");
			cs = CoordinateSpace.forGenome(genome);
		} else if (chrFile != null) {
			cs = new CoordinateSpace(chrFile);
		} else {
			throw new IllegalArgumentException("Provide genome name or coordinate space file");
		}
		BamTranscriptOverlapTag b = new BamTranscriptOverlapTag(p.getStringArg("-f"), cs);
		b.addTag(new File(p.getStringArg("-i")), new File(p.getStringArg("-o")));
		
	}
	
}
