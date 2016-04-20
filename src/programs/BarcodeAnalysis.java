package programs;

import fragment.BarcodedFragmentWithSwitches;
import fragment.BasicBarcodedFragment;
import guttmanlab.core.util.CommandLineParser;
import guttmanlab.core.util.StringParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.Session;

import contact.BarcodeSequence;
import readelement.AnySequence;
import readelement.FragmentBarcode;
import readelement.BarcodeSet;
import readelement.FixedSequence;
import readelement.ReadSequenceElement;
import readelement.Switch;
import readlayout.BarcodedReadLayout;
import readlayout.LigationDesign;
import readlayout.ReadLayout;
import readlayout.ReadLayoutFactory;
import readlayout.ReadLayoutSequenceHash;
import guttmanlab.core.datastructures.Pair;
import guttmanlab.core.pipeline.Job;
import guttmanlab.core.pipeline.JobUtils;
import guttmanlab.core.pipeline.OGSJob;
import guttmanlab.core.pipeline.OGSUtils;
import guttmanlab.core.pipeline.util.FastqParser;
import guttmanlab.core.pipeline.util.FastqSequence;
import guttmanlab.core.pipeline.util.FastqUtils;
import matcher.BitapMatcher;
import matcher.GenericElementMatcher;
import matcher.HashMatcher;

/**
 * Barcode identification and other analyses
 * @author prussell
 *
 */
public final class BarcodeAnalysis {

	private static Logger logger = Logger.getLogger(BarcodeAnalysis.class.getName());
	private static Session drmaaSession;

	private BarcodeAnalysis() {
		// Prevent instantiation
	}
	
	/**
	 * @param p Command line parser
	 * @return The part of the arg string that can be used for a batched job
	 */
	private static String commonArgStringForBatched(CommandLineParser p) {
		Collection<String> removeOptions = new ArrayList<String>();
		removeOptions.add(CommandLineOption.BATCH.getFlag());
		removeOptions.add(CommandLineOption.FASTQ.getFlag());
		removeOptions.add(CommandLineOption.FASTQ_1.getFlag());
		removeOptions.add(CommandLineOption.FASTQ_2.getFlag());
		removeOptions.add(CommandLineOption.OUTPUT_PREFIX.getFlag());
		removeOptions.add(CommandLineOption.SUFFIX_FASTQ.getFlag());
		removeOptions.add(CommandLineOption.SUFFIX_FASTQ_1.getFlag());
		removeOptions.add(CommandLineOption.SUFFIX_FASTQ_2.getFlag());
		return p.getArgString(removeOptions);
	}
	
	/**
	 * @param splitFastqs Split fastq file names
	 * @param outPrefix Output prefix
	 * @return Split output table names by fastq name
	 */
	private static Map<String, String> splitTables(Collection<String> splitFastqs, String outPrefix) {
		Map<String, String> splitTables = new TreeMap<String, String>();
		int i = 0;
		for(String fq : splitFastqs) {
			splitTables.put(fq, outPrefix + "." + i); // Create split table names and index by associated input fastq
			i++;
		}
		return splitTables;
	}
	
	/**
	 * @param splitFastqs1 Split fastq1 file names
	 * @param splitFastqs2 Split fastq2 file names
	 * @param outPrefix Output prefix
	 * @return Split output table names by fastq names
	 */
	private static Map<Pair<String>, String> splitTables(List<String> splitFastqs1, List<String> splitFastqs2, String outPrefix) {
		Map<Pair<String>, String> splitTables = new HashMap<Pair<String>, String>();
		for(int i=0; i<splitFastqs1.size(); i++) {
			splitTables.put(new Pair<String>(splitFastqs1.get(i), splitFastqs2.get(i)), outPrefix + "." + i); // Create split table names and index by associated input fastq
		}
		return splitTables;
	}
	
	/**
	 * @param splitFastqs Split fastq file names
	 * @return Split suffix fastq file names
	 */
	private static Collection<String> splitSuffixFastqs(Collection<String> splitFastqs) {
		Collection<String> splitSuffixFastqs = new ArrayList<String>();
		for(String fq : splitFastqs) {
			splitSuffixFastqs.add(makeSuffixFastqName(fq));
		}
		return splitSuffixFastqs;
	}
	
	private static void catFilesAndDelete(Collection<String> files, String outFile) throws IOException {
		FileWriter w = new FileWriter(outFile);
		for(String file : files) {
			FileReader r = new FileReader(file);
			BufferedReader b = new BufferedReader(r);
			while(b.ready()) {
				w.write(b.readLine() + "\n");
			}
			r.close();
			b.close();
			File f = new File(file);
			f.delete();
		}
		w.close();
	}
	
	/**
	 * For RNA-DNA 3D barcoding method
	 * Identify barcodes in reads and write to a table
	 * Split the fastq file into several smaller files and batch out the writing of the table
	 * @param p CommandLineParser object containing all the options
	 * @throws IOException
	 * @throws DrmaaException
	 * @throws InterruptedException
	 */
	private static void divideFastqAndFindBarcodesSingleFastq(CommandLineParser p) throws IOException, DrmaaException, InterruptedException {
		
		String outPrefix = p.getStringArg(CommandLineOption.OUTPUT_PREFIX.getFlag());
		String email = p.getStringArg(CommandLineOption.EMAIL.getFlag());
		String fastq = p.getStringArg(CommandLineOption.FASTQ.getFlag());
		String jar = p.getStringArg(CommandLineOption.JAR.getFlag());
		int numFastq = p.getIntArg(CommandLineOption.NUM_FASTQ.getFlag());
		boolean splitOutputBySwitchesInLayout = p.getBooleanArg(CommandLineOption.SPLIT_BY_SWITCHES.getFlag());
		String suffixFastq = p.getStringArg(CommandLineOption.SUFFIX_FASTQ.getFlag());
		
		Collection<String> splitFastqs = FastqUtils.divideFastqFile(fastq, numFastq);
		Map<String, String> splitTables = splitTables(splitFastqs, outPrefix);
		Collection<String> splitSuffixFastqs = splitSuffixFastqs(splitFastqs);
		Collection<Job> jobs = new ArrayList<Job>();
		
		for(String fq : splitTables.keySet()) {
			String cmmd = "java -jar -Xmx5g -Xms2g -Xmn1g " + jar + " ";
			cmmd += commonArgStringForBatched(p) + " ";
			cmmd += CommandLineOption.FASTQ.getFlag() + " " + fq + " ";
			cmmd += CommandLineOption.OUTPUT_PREFIX.getFlag() + " " + splitTables.get(fq) + " ";
			cmmd += CommandLineOption.SUFFIX_FASTQ.getFlag() + " " + makeSuffixFastqName(fq) + " ";
			String jobNameWithSlashes = "OGS_job_" + fq;
			String jobName = jobNameWithSlashes.replaceAll("/", "_");
			OGSJob job = new OGSJob(drmaaSession, cmmd, true, jobName, email);
			job.submit();
			jobs.add(job);
		}
		JobUtils.waitForAll(jobs);
		
		if(!splitOutputBySwitchesInLayout) {
			catFilesAndDelete(splitTables.values(), outPrefix);
			if(suffixFastq != null) {
				catFilesAndDelete(splitSuffixFastqs, suffixFastq);
			}
		}
		
	}
	
		
	/**
	 * For RNA-DNA 3D barcoding method
	 * Identify barcodes in reads and write to a table
	 * Split the fastq file into several smaller files and batch out the writing of the table
	 * @param p CommandLineParser object containing all the options
	 * @throws IOException
	 * @throws DrmaaException
	 * @throws InterruptedException
	 */
	private static void divideFastqAndFindBarcodesPairedFastq(CommandLineParser p) throws IOException, DrmaaException, InterruptedException {
		
		String outPrefix = p.getStringArg(CommandLineOption.OUTPUT_PREFIX.getFlag());
		String email = p.getStringArg(CommandLineOption.EMAIL.getFlag());
		String fastq1 = p.getStringArg(CommandLineOption.FASTQ_1.getFlag());
		String fastq2 = p.getStringArg(CommandLineOption.FASTQ_2.getFlag());
		String jar = p.getStringArg(CommandLineOption.JAR.getFlag());
		int numFastq = p.getIntArg(CommandLineOption.NUM_FASTQ.getFlag());
		boolean splitOutputBySwitchesInLayout = p.getBooleanArg(CommandLineOption.SPLIT_BY_SWITCHES.getFlag());
		String suffixFastq1 = p.getStringArg(CommandLineOption.SUFFIX_FASTQ_1.getFlag());
		String suffixFastq2 = p.getStringArg(CommandLineOption.SUFFIX_FASTQ_2.getFlag());
		
		List<String> splitFastqs1 = FastqUtils.divideFastqFile(fastq1, numFastq);
		List<String> splitFastqs2 = FastqUtils.divideFastqFile(fastq2, numFastq);
		Map<Pair<String>, String> splitTables = splitTables(splitFastqs1, splitFastqs2, outPrefix);
		Collection<String> splitSuffixFastqs1 = splitFastqs1 == null ? null : splitSuffixFastqs(splitFastqs1);
		Collection<String> splitSuffixFastqs2 = splitFastqs2 == null ? null : splitSuffixFastqs(splitFastqs2);
		Collection<Job> jobs = new ArrayList<Job>();
		
		for(Pair<String> fqs : splitTables.keySet()) {
			String fq1 = fqs.getValue1();
			String fq2 = fqs.getValue2();
			String cmmd = "java -jar -Xmx5g -Xms2g -Xmn1g " + jar + " ";
			cmmd += commonArgStringForBatched(p) + " ";
			cmmd += CommandLineOption.FASTQ_1.getFlag() + " " + fq1 + " ";
			cmmd += CommandLineOption.FASTQ_2.getFlag() + " " + fq2 + " ";
			cmmd += CommandLineOption.OUTPUT_PREFIX.getFlag() + " " + splitTables.get(fqs) + " ";
			if(suffixFastq1 != null) cmmd += CommandLineOption.SUFFIX_FASTQ_1.getFlag() + " " + makeSuffixFastqName(fq1) + " ";
			if(suffixFastq2 != null) cmmd += CommandLineOption.SUFFIX_FASTQ_2.getFlag() + " " + makeSuffixFastqName(fq2) + " ";
			String jobNameWithSlashes = "OGS_job_" + fq1 + "_" + fq2;
			String jobName = jobNameWithSlashes.replaceAll("/", "_");
			OGSJob job = new OGSJob(drmaaSession, cmmd, true, jobName, email);
			job.submit();
			jobs.add(job);
		}
		JobUtils.waitForAll(jobs);
		
		if(!splitOutputBySwitchesInLayout) {
			catFilesAndDelete(splitTables.values(), outPrefix);
			if(suffixFastq1 != null) {
				catFilesAndDelete(splitSuffixFastqs1, suffixFastq1);
			}
			if(suffixFastq2 != null) {
				catFilesAndDelete(splitSuffixFastqs2, suffixFastq2);
			}
		}
		
	}
	
		
	/**
	 * @param fastq Fastq file
	 * @return Name of fastq file to write read suffixes to (removing matched element section)
	 */
	private static String makeSuffixFastqName(String fastq) {
		return fastq.replaceAll(".fq", "").replaceAll(".fastq", "") + ".suffix.fq";
	}
	
	/**
	 * For RNA-DNA 3D barcoding method
	 * Identify barcodes in reads and write to a table
	 * @param fastq Fastq file
	 * @param layout Barcoded read layout
	 * @param outFilePrefix Output table
	 * @param verbose Verbose table output
	 * @param splitOutputBySwitchesInLayout Write separate tables based on values of switch(es) within the reads
	 * @throws IOException
	 */
	private static void findBarcodes(String fastq, BarcodedReadLayout layout, int maxMismatchBarcode, String outFilePrefix,
			boolean verbose, boolean splitOutputBySwitchesInLayout) throws IOException {
		findBarcodes(fastq, layout, outFilePrefix, verbose, splitOutputBySwitchesInLayout, null);
	}
	
	/**
	 * For RNA-DNA 3D barcoding method
	 * Identify barcodes in reads and write to a table
	 * @param fastq Fastq file
	 * @param layout Barcoded read layout
	 * @param outFile Output table
	 * @param verbose Verbose table output
	 * @param splitOutputBySwitchesInLayout Write separate tables based on values of switch(es) within the reads
	 * @param suffixFastq Also write new fastq file(s) of the reads with all layout elements, or null if not using
	 * and positions before/between them removed. In other words, keep the part of the read after the last matched element.
	 * Obeys the switch scenario, so if using switches, this will also write multiple fastq files, one for each switch
	 * @throws IOException
	 */
	private static void findBarcodes(String fastq, BarcodedReadLayout layout, String outFile, boolean verbose, boolean splitOutputBySwitchesInLayout, 
			String suffixFastq) throws IOException {
		logger.info("");
		logger.info("Identifying barcodes in " + fastq + " and writing to table(s) "+ outFile +"...");
		if(splitOutputBySwitchesInLayout) {
			logger.info("Splitting output by value of switches in reads...");
		}
		if(suffixFastq != null) {
			logger.info("Also writing fastq file(s) of reads without matched elements to " + suffixFastq + "...");
		}
		ReadLayoutSequenceHash hash = new ReadLayoutSequenceHash(layout);
		FileWriter tableWriter = new FileWriter(outFile); // Write to table
		BufferedWriter singleFastqWriter = suffixFastq != null ? new BufferedWriter(new FileWriter(suffixFastq)) : null; // Fastq writer if using and not using switches
		Map<String, FileWriter> switchTableWriters = new HashMap<String, FileWriter>(); // Writers for tables if using switches
		Map<String, BufferedWriter> switchFastqWriters = new HashMap<String, BufferedWriter>(); // Writers for fastq files if using and if using switches
		FastqParser iter = new FastqParser(); // Reader for input fastq file
		iter.start(new File(fastq));
		int numDone = 0;
		while(iter.hasNext()) {
			numDone++;
			if(numDone % 10000 == 0) {
				logger.info("Finished " + numDone + " reads.");
			}
			FastqSequence record = iter.next();
			if(record == null) {
				continue;
			}
			String seq = record.getSequence();
			String name = record.getName();
			String line = StringParser.firstField(name) + "\t";
			HashMatcher matcher = new HashMatcher(layout, seq, hash);
			//BitapMatcher matcher = new BitapMatcher(layout, seq);
			List<List<ReadSequenceElement>> matchedElements = matcher.getMatchedElements();
			if(matchedElements != null) {
				BarcodedFragmentWithSwitches f = new BarcodedFragmentWithSwitches(name, seq, null, layout, null);
				BarcodeSequence barcodes = f.getBarcodes(matchedElements, null);
				if(verbose) line += barcodes.getNumBarcodes() + "\t";
				line += barcodes.toString() + "\t";
				if(verbose) line += seq + "\t";
				// Fastq sequence with layout elements removed if writing fastq
				FastqSequence trimmedRecord = suffixFastq != null ? record.trimFirstNBPs(matcher.matchedElementsLengthInRead()) : null;
				if(splitOutputBySwitchesInLayout) { // Write to switch-specific table file
					Map<Switch, List<FixedSequence>> switchValues = f.getSwitchValues();
					String switchTableName = makeOutTableName(outFile, switchValues); // Create name of switch-specific table file
					if(!switchTableWriters.containsKey(switchTableName)) {
						switchTableWriters.put(switchTableName, new FileWriter(switchTableName));
					}
					switchTableWriters.get(switchTableName).write(line + "\n");
					if(suffixFastq != null) { // Write to switch-specific fastq file
						String suffixFastqName = makeOutFastqName(fastq, switchValues); // Make name of switch-specific fastq file
						if(!switchFastqWriters.containsKey(suffixFastqName)) {
							switchFastqWriters.put(suffixFastqName, new BufferedWriter(new FileWriter(suffixFastqName)));
						}
						trimmedRecord.write(switchFastqWriters.get(suffixFastqName));
					}
				} else {
					tableWriter.write(line + "\n");
					if(suffixFastq != null) trimmedRecord.write(singleFastqWriter);
				}
				f = null;
				seq = null;
				name = null;
				line = null;
				record = null;
				continue;
			}
			seq = null;
			name = null;
			line = null;
			record = null;
		}
		tableWriter.close();
		for(FileWriter fw : switchTableWriters.values()) {
			fw.close();
		}
		if(suffixFastq != null) {
			singleFastqWriter.close();
			for(BufferedWriter fw : switchFastqWriters.values()) {
				fw.close();
			}
		}
	}
	

	/**
	 * Identify barcodes in both reads
	 * @param fastq1 Read 1 fastq
	 * @param fastq2 Read 2 fastq
	 * @param layout1 Read 1 layout
	 * @param layout2 Read 2 layout
	 * @param maxMismatchBarcodeRead1 Max mismatches in barcode in read1
	 * @param maxMismatchBarcodeRead2 Max mismatches in barcode in read2
	 * @param outFile Output table
	 * @param suffixFastq1 Also write new fastq file(s) of the reads with all layout elements, and positions before/between them removed.
	 * In other words, keep the part of the read after the last matched element.
	 * Or null if not using.
	 * @param suffixFastq2
	 * @param verbose
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private static void findBarcodes(String fastq1, String fastq2, BarcodedReadLayout layout1, BarcodedReadLayout layout2, int maxMismatchBarcodeRead1, int maxMismatchBarcodeRead2,
			String outFile, String suffixFastq1, String suffixFastq2, boolean verbose) throws IOException {
		logger.info("");
		logger.info("Identifying barcodes and writing to table(s) "+ outFile +"...");
		if(suffixFastq1 != null) {
			logger.info("Also writing fastq file of read1 minus matched elements to " + suffixFastq1 + "...");
		}
		if(suffixFastq2 != null) {
			logger.info("Also writing fastq file of read2 minus matched elements to " + suffixFastq2 + "...");
		}
		ReadLayoutSequenceHash hash1 = new ReadLayoutSequenceHash(layout1);
		ReadLayoutSequenceHash hash2 = new ReadLayoutSequenceHash(layout2);
		FileWriter tableWriter = new FileWriter(outFile); // Write to table
		BufferedWriter singleFastqWriter1 = suffixFastq1 != null ? new BufferedWriter(new FileWriter(suffixFastq1)) : null; // Fastq writer if using and not using switches
		BufferedWriter singleFastqWriter2 = suffixFastq2 != null ? new BufferedWriter(new FileWriter(suffixFastq2)) : null; // Fastq writer if using and not using switches
		FastqParser iter1 = new FastqParser(); // Reader for input fastq file
		FastqParser iter2 = new FastqParser(); // Reader for input fastq file
		iter1.start(new File(fastq1));
		iter2.start(new File(fastq2));
		int numDone = 0;
		while(iter1.hasNext() && iter2.hasNext()) {
			numDone++;
			if(numDone % 10000 == 0) {
				logger.info("Finished " + numDone + " reads.");
			}
			FastqSequence record1 = iter1.next();
			FastqSequence record2 = iter2.next();
			if(record1 == null && record2 == null) {
				continue;
			}
			String seq1 = record1.getSequence();
			String name1 = StringParser.firstField(record1.getName());
			String seq2 = record2.getSequence();
			String name2 = StringParser.firstField(record2.getName());
			if(!name1.equals(name2)) {
				tableWriter.close();
				throw new IllegalStateException("Paired fastq records out of order: " + name1 + " " + name2);
			}
			String line = name1 + "\t";
			HashMatcher matcher1 = new HashMatcher(layout1, seq1, hash1);
			HashMatcher matcher2 = new HashMatcher(layout2, seq2, hash2);
			//BitapMatcher matcher1 = new BitapMatcher(layout1, seq1);
			//BitapMatcher matcher2 = new BitapMatcher(layout2, seq1);
			List<List<ReadSequenceElement>> matchedElements1 = matcher1.getMatchedElements();
			List<List<ReadSequenceElement>> matchedElements2 = matcher2.getMatchedElements();
			if(matchedElements1 != null || matchedElements2 != null) {
				if(matchedElements1 != null) {
					BasicBarcodedFragment f = new BasicBarcodedFragment(name1, seq1, null, layout1, null);
					BarcodeSequence barcodes = f.getBarcodes(matchedElements1, null);
					line += barcodes.toString();
					FastqSequence trimmedRecord = suffixFastq1 != null ? record1.trimFirstNBPs(matcher1.matchedElementsLengthInRead()) : null;
					if(suffixFastq1 != null) trimmedRecord.write(singleFastqWriter1);
				}
				if(matchedElements2 != null) {
					BasicBarcodedFragment f = new BasicBarcodedFragment(name2, seq2, null, layout2, null);
					BarcodeSequence barcodes = f.getBarcodes(null, matchedElements2);
					line += barcodes.toString() + "\t";
					FastqSequence trimmedRecord = suffixFastq2 != null ? record2.trimFirstNBPs(matcher2.matchedElementsLengthInRead()) : null;
					if(suffixFastq2 != null) trimmedRecord.write(singleFastqWriter2);
				} else {
					line += "\t";
				}
				if(verbose) line += seq1 + "\t" + seq2 + "\t";
				tableWriter.write(line + "\n");
				continue;
			}
		}
		tableWriter.close();
		if(suffixFastq1 != null) {
			singleFastqWriter1.close();
		}
		if(suffixFastq2 != null) {
			singleFastqWriter2.close();
		}
	}
	
	/**
	 * Make name of output table based on prefix and switch values
	 * @param outFilePrefix File prefix
	 * @param switchValues Switch values
	 * @return Output file name
	 */
	private static String makeOutTableName(String outFilePrefix, Map<Switch, List<FixedSequence>> switchValues) {
		String rtrn = outFilePrefix;
		for(Switch s : switchValues.keySet()) {
			for(FixedSequence seq : switchValues.get(s)) {
				rtrn += "_" + seq.getId();
			}
		}
		return rtrn;
	}
	
	/**
	 * Make name of output fastq file based on prefix and switch values
	 * @param outFilePrefix File prefix
	 * @param switchValues Switch values
	 * @return Output file name
	 */
	private static String makeOutFastqName(String outFilePrefix, Map<Switch, List<FixedSequence>> switchValues) {
		String rtrn = outFilePrefix.replaceAll(".fq", "").replaceAll(".fastq", "");
		for(Switch s : switchValues.keySet()) {
			for(FixedSequence seq : switchValues.get(s)) {
				rtrn += "_" + seq.getId();
			}
		}
		rtrn += ".fq";
		return rtrn;
	}
	
	
	/**
	 * Check command line for required options for the given design
	 * @param commandLineParser Command line parser containing all the options
	 */
	private static void validateCommandLine(CommandLineParser commandLineParser) {
		
		String fastq = commandLineParser.getStringArg(CommandLineOption.FASTQ.getFlag());
		String fastq1 = commandLineParser.getStringArg(CommandLineOption.FASTQ_1.getFlag());
		String fastq2 = commandLineParser.getStringArg(CommandLineOption.FASTQ_2.getFlag());
		int maxMismatchBarcodeRead2 = commandLineParser.getIntArg(CommandLineOption.MAX_MISMATCH_EVEN_ODD_BARCODE_READ2.getFlag());
		boolean countBarcodes = commandLineParser.getBooleanArg(CommandLineOption.COUNT_BARCODES.getFlag());
		boolean batch = commandLineParser.getBooleanArg(CommandLineOption.BATCH.getFlag());
		String jar = commandLineParser.getStringArg(CommandLineOption.JAR.getFlag());
		int numFastq = commandLineParser.getIntArg(CommandLineOption.NUM_FASTQ.getFlag());
		String designName = commandLineParser.getStringArg(CommandLineOption.DESIGN_NAME.getFlag());
		LigationDesign design = LigationDesign.fromString(designName);

		if(fastq != null) {
			if(fastq1 != null || fastq2 != null) {
				throw new IllegalArgumentException("Provide single fastq or both paired fastq1, fastq2");
			}
		}
		
		if((fastq1 != null && fastq2 == null) || (fastq1 == null && fastq2 != null)) {
			throw new IllegalArgumentException("Provide single fastq or both paired fastq1, fastq2");
		}
		
		if((countBarcodes) && fastq == null && fastq1 == null && fastq2 == null) {
			throw new IllegalArgumentException("Must provide fastq file to write table of reads and barcodes");
		}
		
		if(batch && jar == null) {
			throw new IllegalArgumentException("Must provide jar file to batch out jobs");
		}
		
		if(batch && numFastq < 1) {
			throw new IllegalArgumentException("Must provide number of fastq files >= 1 to batch out jobs");
		}
		
		switch(design) {
		case PAIRED_DESIGN_APRIL_2016_4BARCODE:
			CommandLineOption.ODD_BARCODE_TABLE.validateCommandLine(commandLineParser);
			CommandLineOption.EVEN_BARCODE_TABLE.validateCommandLine(commandLineParser);
			CommandLineOption.Y_SHAPE_BARCODE_TABLE.validateCommandLine(commandLineParser);
			CommandLineOption.READ2_LENGTH.validateCommandLine(commandLineParser);
			if(maxMismatchBarcodeRead2 < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in barcode in read 2 for " + LigationDesign.PAIRED_DESIGN_APRIL_2016_4BARCODE.toString() + ".");
			}
			CommandLineOption.MAX_MISMATCH_Y_SHAPE_READ2.validateCommandLine(commandLineParser);
			break;
		case PAIRED_DESIGN_APRIL_2016_5BARCODE:
			CommandLineOption.ODD_BARCODE_TABLE.validateCommandLine(commandLineParser);
			CommandLineOption.EVEN_BARCODE_TABLE.validateCommandLine(commandLineParser);
			CommandLineOption.Y_SHAPE_BARCODE_TABLE.validateCommandLine(commandLineParser);
			CommandLineOption.READ2_LENGTH.validateCommandLine(commandLineParser);
			if(maxMismatchBarcodeRead2 < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in barcode in read 2 for " + LigationDesign.PAIRED_DESIGN_APRIL_2016_5BARCODE.toString() + ".");
			}
			CommandLineOption.MAX_MISMATCH_Y_SHAPE_READ2.validateCommandLine(commandLineParser);
			break;
		default:
			throw new UnsupportedOperationException("Not implemented for " + design.toString());		
		}
	}
	
	private enum CommandLineOption {
		
		BATCH("-bt", "Batch out writing of barcode table", null) {
			public void addToCommandLineParser(CommandLineParser p) {p.addBooleanArg(getFlag(), getDescription(), false, false);}
			public void validateCommandLine(CommandLineParser p) {}
		},
		COUNT_BARCODES("-cb", "Count barcodes and write to table", null) {
			public void addToCommandLineParser(CommandLineParser p) {p.addBooleanArg(getFlag(), getDescription(), false, false);}
			public void validateCommandLine(CommandLineParser p) {}
		},
		DESIGN_NAME("-l", "Ligation design. Options: " + LigationDesign.getNamesAsCommaSeparatedList(), "Must provide ligation design name") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), true);}
			public void validateCommandLine(CommandLineParser p) {
				String arg  = p.getStringArg(getFlag());
				if(arg == null) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		DEBUG("-d", "Debug logging on", null) {
			void addToCommandLineParser(CommandLineParser p) {p.addBooleanArg(getFlag(), getDescription(), false, false);}
			public void validateCommandLine(CommandLineParser p) {}
		},
		DPM("-dpm", "Single fixed DPM sequence", "Must provide DPM sequence") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), false, null);}
			public void validateCommandLine(CommandLineParser p) {
				String arg = p.getStringArg(getFlag());
				if(arg == null) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		EMAIL("-e", "Email address for OGS jobs", "Must provide email address") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), false, null);}
			public void validateCommandLine(CommandLineParser p) {
				String arg = p.getStringArg(getFlag());
				if(arg == null) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		ENFORCE_ODD_EVEN("-oe", "Enforce odd/even alternation for barcodes", null) {
			void addToCommandLineParser(CommandLineParser p) {p.addBooleanArg(getFlag(), getDescription(), false, false);}
			public void validateCommandLine(CommandLineParser p) {}
		},
		EVEN_BARCODE_TABLE("-eb", "Even barcode table file (format: barcode_id	barcode_seq)", "Must provide file containing list of even barcodes") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), false, null);}
			public void validateCommandLine(CommandLineParser p) {
				String arg = p.getStringArg(getFlag());
				if(arg == null) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		FASTQ("-fq", "Fastq file if only identifying barcodes in one mate", "Must provide fastq file") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), false, null);}
			public void validateCommandLine(CommandLineParser p) {
				String arg = p.getStringArg(getFlag());
				if(arg == null) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		FASTQ_1("-fq1", "Fastq file read 1 if identifying barcodes in both mates", "Must provide fastq file for read 1") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), false, null);}
			public void validateCommandLine(CommandLineParser p) {
				String arg = p.getStringArg(getFlag());
				if(arg == null) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		FASTQ_2("-fq2", "Fastq file read 2 if identifying barcodes in both mates", "Must provide fastq file for read 2") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), false, null);}
			public void validateCommandLine(CommandLineParser p) {
				String arg = p.getStringArg(getFlag());
				if(arg == null) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		JAR("-j", "This jar file for batched jobs", "") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), false, null);}
			public void validateCommandLine(CommandLineParser p) {
				String arg = p.getStringArg(getFlag());
				if(arg == null) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		LAST_NUM_BARCODES("-nlb", "Number of last barcodes to get for get last N barcodes option", "Must provide number of last barcodes to get") {
			void addToCommandLineParser(CommandLineParser p) {p.addIntArg(getFlag(), getDescription(), false, -1);}
			public void validateCommandLine(CommandLineParser p) {
				int arg = p.getIntArg(getFlag());
				if(arg < 0) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		MAX_MISMATCH_Y_SHAPE_READ2("-mmyr2", "Max mismatches in Y shape barcode in read 2 if identifying barcodes in both mates", "Must provide max mismatches in Y shape barcode in read 2") {
			void addToCommandLineParser(CommandLineParser p) {p.addIntArg(getFlag(), getDescription(), false, -1);}
			public void validateCommandLine(CommandLineParser p) {
				int arg = p.getIntArg(getFlag());
				if(arg < 0) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		OUTPUT_PREFIX("-op", "Output file prefix", "Must provide output file prefix") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), true);}
			public void validateCommandLine(CommandLineParser p) {
				// No validation needed; argument is required
			}
		},
		READ2_LENGTH("-r2l", "Read 2 length", "Must provide read 2 length") {
			void addToCommandLineParser(CommandLineParser p) {p.addIntArg(getFlag(), getDescription(), false, -1);}
			public void validateCommandLine(CommandLineParser p) {
				int arg = p.getIntArg(getFlag());
				if(arg < 0) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		ODD_BARCODE_TABLE("-ob", "Odd barcode table file (format: barcode_id	barcode_seq)", "Must provide table of odd barcodes") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), false, null);}
			public void validateCommandLine(CommandLineParser p) {
				String arg = p.getStringArg(getFlag());
				if(arg == null) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		Y_SHAPE_BARCODE_TABLE("-yb", "Y shape barcode table file (format: barcode_id	barcode_seq)", "Must provide table of Y shape barcodes") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), false, null);}
			public void validateCommandLine(CommandLineParser p) {
				String arg = p.getStringArg(getFlag());
				if(arg == null) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		MAX_MISMATCH_EVEN_ODD_BARCODE_READ2("-mmeor2", "Max mismatches in even/odd barcode in read 2 if identifying barcodes in both mates", 
				"Must provide max mismatches in even/odd barcode in read 2") {
			void addToCommandLineParser(CommandLineParser p) {p.addIntArg(getFlag(), getDescription(), false, -1);}
			public void validateCommandLine(CommandLineParser p) {
				int arg = p.getIntArg(getFlag());
				if(arg < 0) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		NUM_FASTQ("-nf", "Number of fastq files to divide into if batching", "Must specify number of fastq files to divide into") {
			void addToCommandLineParser(CommandLineParser p) {p.addIntArg(getFlag(), getDescription(), false, 20);}
			public void validateCommandLine(CommandLineParser p) {}
		},
		SPLIT_BY_SWITCHES("-ss", "Split output files by switch values in reads", "Must specify whether to split output by switches in reads") {
			void addToCommandLineParser(CommandLineParser p) {p.addBooleanArg(getFlag(), getDescription(), false, false);}
			public void validateCommandLine(CommandLineParser p) {}
		},
		SUFFIX_FASTQ("-wsf", "Also write fastq file of the part of each read after the last matched layout element, if only identifying barcodes in one mate",
				"Must provide output suffix fastq file") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), false, null);}
			public void validateCommandLine(CommandLineParser p) {
				String arg = p.getStringArg(getFlag());
				if(arg == null) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		SUFFIX_FASTQ_1("-wsf1", "Also write fastq file of the part of each read 1 after the last matched layout element, if identifying barcodes in both mates",
				"Must provide output suffix fastq file for read 1") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), false, null);}
			public void validateCommandLine(CommandLineParser p) {
				String arg = p.getStringArg(getFlag());
				if(arg == null) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		SUFFIX_FASTQ_2("-wsf2", "Also write fastq file of the part of each read 2 after the last matched layout element, if identifying barcodes in both mates",
				"Must provide output suffix fastq file for read 2") {
			void addToCommandLineParser(CommandLineParser p) {p.addStringArg(getFlag(), getDescription(), false, null);}
			public void validateCommandLine(CommandLineParser p) {
				String arg = p.getStringArg(getFlag());
				if(arg == null) throw new IllegalArgumentException(getErrorMessage());
			}
		},
		VERBOSE_OUTPUT("-v", "Use verbose output in barcode identification table", "Must specify whether to provide verbose output") {
			void addToCommandLineParser(CommandLineParser p) {p.addBooleanArg(getFlag(), getDescription(), false, false);}
			public void validateCommandLine(CommandLineParser p) {}
		}
		;
		
		private String flag;
		private String description;
		private String errorMessage;
		
		private CommandLineOption(String flag, String description, String errorMessage) {
			this.flag = flag;
			this.description = description;
			this.errorMessage = errorMessage;
		}
		
		abstract void addToCommandLineParser(CommandLineParser p);
		
		public abstract void validateCommandLine(CommandLineParser p);
		
		public static CommandLineParser createCommandLineParser() {
			CommandLineParser rtrn = new CommandLineParser();
			for(CommandLineOption option : values()) {
				option.addToCommandLineParser(rtrn);
			}
			return rtrn;
		}
		
		public String getFlag() {return flag;}
		public String getDescription() {return description;}
		public String getErrorMessage() {return errorMessage;}
		
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws DrmaaException 
	 */
	public static void main(String[] args) throws IOException, DrmaaException, InterruptedException {
		
		logger.info("Barcode analysis starting...");
		
		try {
			drmaaSession = OGSUtils.getDrmaaSession();
		} catch(UnsatisfiedLinkError e) {
			logger.warn("");
			logger.warn("************************************************************");
			logger.warn("WARNING: DRMAA not working. Batching to OGS will not work.");
			logger.warn(e.getMessage());
			logger.warn("************************************************************");
			logger.warn("");
		}
		
		CommandLineParser p = CommandLineOption.createCommandLineParser();
		p.parse(args);
		if(p.getBooleanArg(CommandLineOption.DEBUG.getFlag())) {
			ReadLayout.logger.setLevel(Level.DEBUG);
			BarcodeSet.logger.setLevel(Level.DEBUG);
			ReadLayoutFactory.logger.setLevel(Level.DEBUG);
			BarcodeSequence.logger.setLevel(Level.DEBUG);
			FragmentBarcode.logger.setLevel(Level.DEBUG);
			BasicBarcodedFragment.logger.setLevel(Level.DEBUG);
			FixedSequence.logger.setLevel(Level.DEBUG);
			AnySequence.logger.setLevel(Level.DEBUG);
			BarcodeAnalysis.logger.setLevel(Level.DEBUG);
			GenericElementMatcher.logger.setLevel(Level.DEBUG);
			BitapMatcher.logger.setLevel(Level.DEBUG);
			HashMatcher.logger.setLevel(Level.DEBUG);
			ReadLayoutSequenceHash.logger.setLevel(Level.DEBUG);
		}
		
		String outPrefix = p.getStringArg(CommandLineOption.OUTPUT_PREFIX.getFlag());
		String fastq = p.getStringArg(CommandLineOption.FASTQ.getFlag());
		String fastq1 = p.getStringArg(CommandLineOption.FASTQ_1.getFlag());
		String fastq2 = p.getStringArg(CommandLineOption.FASTQ_2.getFlag());
		int read2Length = p.getIntArg(CommandLineOption.READ2_LENGTH.getFlag());
		String oddBarcodeList = p.getStringArg(CommandLineOption.ODD_BARCODE_TABLE.getFlag());
		String evenBarcodeList = p.getStringArg(CommandLineOption.EVEN_BARCODE_TABLE.getFlag());
		String yShapeBarcodeList = p.getStringArg(CommandLineOption.Y_SHAPE_BARCODE_TABLE.getFlag());
		int maxMismatchEvenOddBarcodeRead2 = p.getIntArg(CommandLineOption.MAX_MISMATCH_EVEN_ODD_BARCODE_READ2.getFlag());
		int maxMismatchYShapeBarcodeRead2 = p.getIntArg(CommandLineOption.MAX_MISMATCH_Y_SHAPE_READ2.getFlag());
		boolean countBarcodes = p.getBooleanArg(CommandLineOption.COUNT_BARCODES.getFlag());
		boolean verbose = p.getBooleanArg(CommandLineOption.VERBOSE_OUTPUT.getFlag());
		boolean batch = p.getBooleanArg(CommandLineOption.BATCH.getFlag());
		String designName = p.getStringArg(CommandLineOption.DESIGN_NAME.getFlag());
		LigationDesign design = LigationDesign.fromString(designName);
		
		validateCommandLine(p);
		
		// Make table of reads IDs and barcodes
		if(outPrefix != null) {
			if(batch) {
				if(fastq != null) divideFastqAndFindBarcodesSingleFastq(p);
				if(fastq1 != null && fastq2 != null) divideFastqAndFindBarcodesPairedFastq(p);
			} else {
				switch(design) {
				case PAIRED_DESIGN_APRIL_2016_4BARCODE:
					BarcodedReadLayout layout7read2 = ReadLayoutFactory.getRead2LayoutRnaDna3DPairedDesignApril2016(evenBarcodeList, oddBarcodeList, 
							yShapeBarcodeList, maxMismatchEvenOddBarcodeRead2, maxMismatchYShapeBarcodeRead2, read2Length, 4);
					findBarcodes(fastq2, layout7read2, maxMismatchEvenOddBarcodeRead2, outPrefix, verbose, false);
					break;
				case PAIRED_DESIGN_APRIL_2016_5BARCODE:
					BarcodedReadLayout layout8read2 = ReadLayoutFactory.getRead2LayoutRnaDna3DPairedDesignApril2016(evenBarcodeList, oddBarcodeList, 
							yShapeBarcodeList, maxMismatchEvenOddBarcodeRead2, maxMismatchYShapeBarcodeRead2, read2Length, 5);
					findBarcodes(fastq2, layout8read2, maxMismatchEvenOddBarcodeRead2, outPrefix, verbose, false);
					break;
				default:
					throw new IllegalArgumentException("Not implemented");
				}
			}
		}

		// Make histogram of barcodes
		if(countBarcodes) {
			switch(design) {
			default:
				throw new IllegalArgumentException("Not implemented");			
			}
		}
			
		logger.info("");
		logger.info("All done.");
				
	}

}
