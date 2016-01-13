package programs;

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

import readelement.AnySequence;
import readelement.Barcode;
import readelement.BarcodeSet;
import readelement.FixedSequence;
import readelement.ReadSequenceElement;
import readelement.Switch;
import readlayout.BarcodedReadLayout;
import readlayout.LigationDesign;
import readlayout.ReadLayout;
import readlayout.ReadLayoutFactory;
import readlayout.ReadLayoutSequenceHash;
import sequentialbarcode.BarcodeSequence;
import sequentialbarcode.BarcodedDNAFragment;
import sequentialbarcode.BarcodedFragment;
import sequentialbarcode.BarcodedFragmentImpl;
import sequentialbarcode.BarcodedFragmentWithSwitches;
import sequentialbarcode.BarcodedRNAFragment;
import guttmanlab.core.pipeline.Job;
import guttmanlab.core.pipeline.JobUtils;
import guttmanlab.core.pipeline.OGSJob;
import guttmanlab.core.pipeline.util.FastqUtils;
import matcher.BitapMatcher;
import matcher.GenericElementMatcher;
import matcher.HashMatcher;
import nextgen.core.pipeline.util.OGSUtils;
import broad.core.datastructures.Pair;
import broad.pda.seq.fastq.FastqParser;
import broad.pda.seq.fastq.FastqSequence;

/**
 * Barcode identification and other analyses
 * @author prussell
 *
 */
public class BarcodeAnalysis {

	public static Logger logger = Logger.getLogger(BarcodeAnalysis.class.getName());
	private static Session drmaaSession;
	private static boolean GET_LAST_BARCODES = false;
	private static int NUM_LAST_BARCODES = Integer.MAX_VALUE;
	private static int MAX_MISMATCH_BARCODE;
	private static int MAX_MISMATCH_BARCODE_READ1;
	private static int MAX_MISMATCH_EVEN_ODD_BARCODE_READ2;
	private static int MAX_MISMATCH_Y_SHAPE_BARCODE_READ2;
	private static int MAX_MISMATCH_RPM;
	private static int MAX_MISMATCH_DPM;
	private static int MAX_MISMATCH_ADAPTER;

	/**
	 * @param p Command line parser
	 * @return The part of the arg string that can be used for a batched job
	 */
	private static String commonArgStringForBatched(CommandLineParser p) {
		Collection<String> removeOptions = new ArrayList<String>();
		removeOptions.add(batchOption);
		removeOptions.add(fastqOption);
		removeOptions.add(fastq1Option);
		removeOptions.add(fastq2Option);
		removeOptions.add(outPrefixOption);
		removeOptions.add(suffixFastqOption);
		removeOptions.add(suffixFastq1Option);
		removeOptions.add(suffixFastq2Option);
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
		
		String outPrefix = p.getStringArg(outPrefixOption);
		String email = p.getStringArg(emailOption);
		String fastq = p.getStringArg(fastqOption);
		String jar = p.getStringArg(jarOption);
		int numFastq = p.getIntArg(numFastqOption);
		boolean splitOutputBySwitchesInLayout = p.getBooleanArg(splitOutFilesBySwitchesOption);
		String suffixFastq = p.getStringArg(suffixFastqOption);
		
		Collection<String> splitFastqs = FastqUtils.divideFastqFile(fastq, numFastq);
		Map<String, String> splitTables = splitTables(splitFastqs, outPrefix);
		Collection<String> splitSuffixFastqs = splitSuffixFastqs(splitFastqs);
		Collection<Job> jobs = new ArrayList<Job>();
		
		for(String fq : splitTables.keySet()) {
			String cmmd = "java -jar -Xmx5g -Xms2g -Xmn1g " + jar + " ";
			cmmd += commonArgStringForBatched(p) + " ";
			cmmd += fastqOption + " " + fq + " ";
			cmmd += outPrefixOption + " " + splitTables.get(fq) + " ";
			cmmd += suffixFastqOption + " " + makeSuffixFastqName(fq) + " ";
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
		
		String outPrefix = p.getStringArg(outPrefixOption);
		String email = p.getStringArg(emailOption);
		String fastq1 = p.getStringArg(fastq1Option);
		String fastq2 = p.getStringArg(fastq2Option);
		String jar = p.getStringArg(jarOption);
		int numFastq = p.getIntArg(numFastqOption);
		boolean splitOutputBySwitchesInLayout = p.getBooleanArg(splitOutFilesBySwitchesOption);
		String suffixFastq1 = p.getStringArg(suffixFastq1Option);
		String suffixFastq2 = p.getStringArg(suffixFastq2Option);
		
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
			cmmd += fastq1Option + " " + fq1 + " ";
			cmmd += fastq2Option + " " + fq2 + " ";
			cmmd += outPrefixOption + " " + splitTables.get(fqs) + " ";
			if(suffixFastq1 != null) cmmd += suffixFastq1Option + " " + makeSuffixFastqName(fq1) + " ";
			if(suffixFastq2 != null) cmmd += suffixFastq2Option + " " + makeSuffixFastqName(fq2) + " ";
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
	private static void findBarcodes(String fastq, BarcodedReadLayout layout, String outFilePrefix, boolean verbose, boolean splitOutputBySwitchesInLayout) throws IOException {
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
	private static void findBarcodes(String fastq, BarcodedReadLayout layout, String outFile, boolean verbose, boolean splitOutputBySwitchesInLayout, String suffixFastq) throws IOException {
		logger.info("");
		logger.info("Identifying barcodes and writing to table(s) "+ outFile +"...");
		if(splitOutputBySwitchesInLayout) {
			logger.info("Splitting output by value of switches in reads...");
		}
		if(suffixFastq != null) {
			logger.info("Also writing fastq file(s) of reads without matched elements to " + suffixFastq + "...");
		}
		ReadLayoutSequenceHash hash = new ReadLayoutSequenceHash(layout, MAX_MISMATCH_BARCODE);
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
			record.removeAtSymbolFromName();
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
				if(GET_LAST_BARCODES) {
					BarcodeSequence lastBarcodes = barcodes.getLastBarcodes(NUM_LAST_BARCODES);
					line += lastBarcodes.toString() + "\t";
				}
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
	 * @param outFile Output table
	 * @param suffixFastq1 Also write new fastq file(s) of the reads with all layout elements, and positions before/between them removed.
	 * In other words, keep the part of the read after the last matched element.
	 * Or null if not using.
	 * @param suffixFastq2
	 * @param verbose
	 * @throws IOException
	 */
	private static void findBarcodes(String fastq1, String fastq2, BarcodedReadLayout layout1, BarcodedReadLayout layout2, String outFile, String suffixFastq1, String suffixFastq2, boolean verbose) throws IOException {
		logger.info("");
		logger.info("Identifying barcodes and writing to table(s) "+ outFile +"...");
		if(suffixFastq1 != null) {
			logger.info("Also writing fastq file of read1 minus matched elements to " + suffixFastq1 + "...");
		}
		if(suffixFastq2 != null) {
			logger.info("Also writing fastq file of read2 minus matched elements to " + suffixFastq2 + "...");
		}
		ReadLayoutSequenceHash hash1 = new ReadLayoutSequenceHash(layout1, MAX_MISMATCH_BARCODE_READ1);
		ReadLayoutSequenceHash hash2 = new ReadLayoutSequenceHash(layout2, MAX_MISMATCH_EVEN_ODD_BARCODE_READ2);
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
			record1.removeAtSymbolFromName();
			String seq1 = record1.getSequence();
			String name1 = StringParser.firstField(record1.getName());
			record2.removeAtSymbolFromName();
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
					BarcodedFragmentImpl f = new BarcodedFragmentImpl(name1, seq1, null, layout1, null);
					BarcodeSequence barcodes = f.getBarcodes(matchedElements1, null);
					line += barcodes.toString();
					FastqSequence trimmedRecord = suffixFastq1 != null ? record1.trimFirstNBPs(matcher1.matchedElementsLengthInRead()) : null;
					if(suffixFastq1 != null) trimmedRecord.write(singleFastqWriter1);
				}
				if(matchedElements2 != null) {
					BarcodedFragmentImpl f = new BarcodedFragmentImpl(name2, seq2, null, layout2, null);
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
	 * For RNA-DNA 3D barcoding method
	 * Count instances of each barcode and print totals
	 * @param fastq Fastq file
	 * @param layout Barcoded read layout
	 * @throws IOException
	 */
	private static void countBarcodes(String fastq, BarcodedReadLayout layout) throws IOException {
		logger.info("");
		logger.info("Counting read2 barcodes for RNA-DNA-3D...");
		Map<Barcode, Integer> barcodeCounts = new TreeMap<Barcode, Integer>();
		for(Barcode b : layout.getAllBarcodes()) {
			barcodeCounts.put(b, Integer.valueOf(0));
		}
		ReadLayoutSequenceHash hash = new ReadLayoutSequenceHash(layout, MAX_MISMATCH_BARCODE);
		FastqParser iter = new FastqParser();
		iter.start(new File(fastq));
		int numDone = 0;
		while(iter.hasNext()) {
			FastqSequence record = iter.next();
			String seq = record.getSequence();
			String name = record.getName();
			if(new HashMatcher(layout, seq, hash).getMatchedElements() != null) {
			//if(new BitapMatcher(layout, seq).getMatchedElements() != null) {
				BarcodedFragment f = new BarcodedFragmentImpl(name, null, seq, null, layout);
				BarcodeSequence barcodes = f.getBarcodes();
				for(Barcode b : barcodes.getBarcodes()) {
					barcodeCounts.put(b, Integer.valueOf(barcodeCounts.get(b).intValue() + 1));
				}
			}
			if(numDone > 0 && numDone % 10000 == 0) {
				logger.info("");
				logger.info("Finished " + numDone + " reads.");
				for(Barcode b : barcodeCounts.keySet()) {
					System.out.println(b.getId() + "\t" + b.getSequence() + "\t" + barcodeCounts.get(b).intValue());
				}
			}
			numDone++;
		}
	}
	
	/**
	 * Check command line for required options for the given design
	 * @param commandLineParser Command line parser containing all the options
	 */
	private static void validateCommandLine(CommandLineParser commandLineParser) {
		
		String outPrefix = commandLineParser.getStringArg(outPrefixOption);
		String fastq = commandLineParser.getStringArg(fastqOption);
		String fastq1 = commandLineParser.getStringArg(fastq1Option);
		String fastq2 = commandLineParser.getStringArg(fastq2Option);
		int readLength = commandLineParser.getIntArg(readLengthOption);
		int read1Length = commandLineParser.getIntArg(read1LengthOption);
		int read2Length = commandLineParser.getIntArg(read2LengthOption);
		String oddBarcodeList = commandLineParser.getStringArg(oddBarcodeTableOption);
		String evenBarcodeList = commandLineParser.getStringArg(evenBarcodeTableOption);
		String yShapeBarcodeList = commandLineParser.getStringArg(yShapeBarcodeTableOption);
		String rpm = commandLineParser.getStringArg(rpmOption);
		String dpm = commandLineParser.getStringArg(dpmOption);
		int maxMismatchBarcode = commandLineParser.getIntArg(maxMismatchBarcodeOption);
		int maxMismatchBarcodeRead1 = commandLineParser.getIntArg(maxMismatchBarcodeRead1Option);
		int maxMismatchBarcodeRead2 = commandLineParser.getIntArg(maxMismatchEvenOddBarcodeRead2Option);
		int maxMismatchYShapeRead2 = commandLineParser.getIntArg(maxMismatchYShapeBarcodeRead2Option);
		int maxMismatchRpm = commandLineParser.getIntArg(maxMismatchRpmOption);
		int maxMismatchDpm = commandLineParser.getIntArg(maxMismatchDpmOption);
		boolean countBarcodes = commandLineParser.getBooleanArg(countBarcodesOption);
		boolean batch = commandLineParser.getBooleanArg(batchOption);
		String jar = commandLineParser.getStringArg(jarOption);
		int numFastq = commandLineParser.getIntArg(numFastqOption);
		String designName = commandLineParser.getStringArg(designNameOption);
		LigationDesign design = LigationDesign.fromString(designName);
		int totalNumBarcodes = commandLineParser.getIntArg(totalNumBarcodesOption);		
		int totalNumBarcodesRead2 = commandLineParser.getIntArg(totalNumBarcodesRead2Option);		
		int maxMismatchAdapter = commandLineParser.getIntArg(maxMismatchAdapterOption);
		String adapterSeqFasta = commandLineParser.getStringArg(adapterSeqFastaOption);
		@SuppressWarnings("unused")
		boolean splitOutputBySwitchesInLayout = commandLineParser.getBooleanArg(splitOutFilesBySwitchesOption);
		String may2015firstBarcodeFile = commandLineParser.getStringArg(may2015firstBarcodeFileOption);
		String may2015OddBarcodeFile = commandLineParser.getStringArg(may2015oddBarcodeFileOption);
		String may2015evenBarcodeFile = commandLineParser.getStringArg(may2015evenBarcodeFileOption);
		String may2015lastBarcodeFile = commandLineParser.getStringArg(may2015lastBarcodeFileOption);
		int may2015maxMismatchFirstBarcode = commandLineParser.getIntArg(may2015maxMismatchFirstBarcodeOption);
		int may2015maxMismatchLastBarcode = commandLineParser.getIntArg(may2015maxMismatchLastBarcodeOption);
		int may2015maxMismatchOddEvenBarcode = commandLineParser.getIntArg(may2015maxMismatchOddEvenBarcodeOption);
		String july2015firstBarcodeFile = commandLineParser.getStringArg(july2015read1BarcodeFileOption);

		if(fastq != null) {
			if(fastq1 != null || fastq2 != null) {
				throw new IllegalArgumentException("Provide single fastq or both paired fastq1, fastq2");
			}
		}
		
		if((fastq1 != null && fastq2 == null) || (fastq1 == null && fastq2 != null)) {
			throw new IllegalArgumentException("Provide single fastq or both paired fastq1, fastq2");
		}
		
		if((outPrefix != null || countBarcodes) && fastq == null && fastq1 == null && fastq2 == null) {
			throw new IllegalArgumentException("Must provide fastq file to write table of reads and barcodes");
		}
		
		if(batch && jar == null) {
			throw new IllegalArgumentException("Must provide jar file to batch out jobs");
		}
		
		if(batch && numFastq < 1) {
			throw new IllegalArgumentException("Must provide number of fastq files >= 1 to batch out jobs");
		}
		
		switch(design) {
		case PAIRED_DESIGN_BARCODE_IN_READ2:
			if(oddBarcodeList == null) {
				throw new IllegalArgumentException("Must provide odd barcode list for " + LigationDesign.PAIRED_DESIGN_BARCODE_IN_READ2.toString() + ".");
			}
			if(evenBarcodeList == null) {
				throw new IllegalArgumentException("Must provide even barcode list for " + LigationDesign.PAIRED_DESIGN_BARCODE_IN_READ2.toString() + ".");
			}
			if(totalNumBarcodes < 1) {
				throw new IllegalArgumentException("Must provide number of barcode ligations for " + LigationDesign.PAIRED_DESIGN_BARCODE_IN_READ2.toString() + ".");
			}
			if(rpm == null) {
				throw new IllegalArgumentException("Must provide RPM sequence for " + LigationDesign.PAIRED_DESIGN_BARCODE_IN_READ2.toString() + ".");
			}
			if(readLength < 1) {
				throw new IllegalArgumentException("Must provide read length for " + LigationDesign.PAIRED_DESIGN_BARCODE_IN_READ2.toString() + ".");
			}
			if(maxMismatchBarcode < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in barcode for " + LigationDesign.PAIRED_DESIGN_BARCODE_IN_READ2.toString() + ".");
			}
			if(maxMismatchRpm < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in RPM for " + LigationDesign.PAIRED_DESIGN_BARCODE_IN_READ2.toString() + ".");
			}
			break;
		case PAIRED_DESIGN_JULY_2015:
			if(fastq1 == null) {
				throw new IllegalArgumentException("Must provide fastq file 1 for " + LigationDesign.PAIRED_DESIGN_JULY_2015.toString() + ".");
			}
			if(fastq2 == null) {
				throw new IllegalArgumentException("Must provide fastq file 2 for " + LigationDesign.PAIRED_DESIGN_JULY_2015.toString() + ".");
			}
			if(oddBarcodeList == null) {
				throw new IllegalArgumentException("Must provide odd barcode list (for read 2) for " + LigationDesign.PAIRED_DESIGN_JULY_2015.toString() + ".");
			}
			if(evenBarcodeList == null) {
				throw new IllegalArgumentException("Must provide even barcode list (for read 2) for " + LigationDesign.PAIRED_DESIGN_JULY_2015.toString() + ".");
			}
			if(july2015firstBarcodeFile == null) {
				throw new IllegalArgumentException("Must provide read 1 barcode list for " + LigationDesign.PAIRED_DESIGN_JULY_2015.toString() + ".");
			}
			if(totalNumBarcodesRead2 < 1) {
				throw new IllegalArgumentException("Must provide number of barcode ligations for " + LigationDesign.PAIRED_DESIGN_JULY_2015.toString() + ".");
			}
			if(read1Length < 1) {
				throw new IllegalArgumentException("Must provide read 1 length for " + LigationDesign.PAIRED_DESIGN_JULY_2015.toString() + ".");
			}
			if(read2Length < 1) {
				throw new IllegalArgumentException("Must provide read 2 length for " + LigationDesign.PAIRED_DESIGN_JULY_2015.toString() + ".");
			}
			if(maxMismatchBarcodeRead1 < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in barcode in read 1 for " + LigationDesign.PAIRED_DESIGN_JULY_2015.toString() + ".");
			}
			if(maxMismatchBarcodeRead2 < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in barcode in read 2 for " + LigationDesign.PAIRED_DESIGN_JULY_2015.toString() + ".");
			}
			if(maxMismatchYShapeRead2 < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in Y shape barcode in read 2 for " + LigationDesign.PAIRED_DESIGN_JULY_2015.toString() + ".");
			}
			break;
		case SINGLE_DESIGN_WITH_SWITCH:
			if(oddBarcodeList == null) {
				throw new IllegalArgumentException("Must provide odd barcode list for " + LigationDesign.SINGLE_DESIGN_WITH_SWITCH.toString() + ".");
			}
			if(evenBarcodeList == null) {
				throw new IllegalArgumentException("Must provide even barcode list for " + LigationDesign.SINGLE_DESIGN_WITH_SWITCH.toString() + ".");
			}
			if(totalNumBarcodes < 1) {
				throw new IllegalArgumentException("Must provide number of barcode ligations for " + LigationDesign.SINGLE_DESIGN_WITH_SWITCH.toString() + ".");
			}
			if(rpm == null) {
				throw new IllegalArgumentException("Must provide RPM sequence for " + LigationDesign.SINGLE_DESIGN_WITH_SWITCH.toString() + ".");
			}
			if(dpm == null) {
				throw new IllegalArgumentException("Must provide DPM sequence for " + LigationDesign.SINGLE_DESIGN_WITH_SWITCH.toString() + ".");
			}
			if(readLength < 1) {
				throw new IllegalArgumentException("Must provide read length for " + LigationDesign.SINGLE_DESIGN_WITH_SWITCH.toString() + ".");
			}
			if(maxMismatchBarcode < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in barcode for " + LigationDesign.SINGLE_DESIGN_WITH_SWITCH.toString() + ".");
			}
			if(maxMismatchRpm < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in RPM for " + LigationDesign.SINGLE_DESIGN_WITH_SWITCH.toString() + ".");
			}
			if(maxMismatchDpm < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in DPM for " + LigationDesign.SINGLE_DESIGN_WITH_SWITCH.toString() + ".");
			}
			break;
		case SINGLE_DESIGN:
			if(oddBarcodeList == null) {
				throw new IllegalArgumentException("Must provide odd barcode list for " + LigationDesign.SINGLE_DESIGN.toString() + ".");
			}
			if(evenBarcodeList == null) {
				throw new IllegalArgumentException("Must provide even barcode list for " + LigationDesign.SINGLE_DESIGN.toString() + ".");
			}
			if(totalNumBarcodes < 1) {
				throw new IllegalArgumentException("Must provide number of barcode ligations for " + LigationDesign.SINGLE_DESIGN.toString() + ".");
			}
			if(dpm == null) {
				throw new IllegalArgumentException("Must provide DPM sequence for " + LigationDesign.SINGLE_DESIGN.toString() + ".");
			}
			if(readLength < 1) {
				throw new IllegalArgumentException("Must provide read length for " + LigationDesign.SINGLE_DESIGN.toString() + ".");
			}
			if(maxMismatchBarcode < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in barcode for " + LigationDesign.SINGLE_DESIGN.toString() + ".");
			}
			if(maxMismatchDpm < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in DPM for " + LigationDesign.SINGLE_DESIGN.toString() + ".");
			}
			break;
		case SINGLE_DESIGN_MARCH_2015:
			if(adapterSeqFasta == null) {
				throw new IllegalArgumentException("Must provide fasta file of adapter sequences for " + LigationDesign.SINGLE_DESIGN_MARCH_2015.toString() + ".");
			}
			if(oddBarcodeList == null) {
				throw new IllegalArgumentException("Must provide odd barcode list for " + LigationDesign.SINGLE_DESIGN_MARCH_2015.toString() + ".");
			}
			if(evenBarcodeList == null) {
				throw new IllegalArgumentException("Must provide even barcode list for " + LigationDesign.SINGLE_DESIGN_MARCH_2015.toString() + ".");
			}
			if(totalNumBarcodes < 1) {
				throw new IllegalArgumentException("Must provide number of barcode ligations for " + LigationDesign.SINGLE_DESIGN_MARCH_2015.toString() + ".");
			}
			if(readLength < 1) {
				throw new IllegalArgumentException("Must provide read length for " + LigationDesign.SINGLE_DESIGN_MARCH_2015.toString() + ".");
			}
			if(maxMismatchBarcode < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in barcode for " + LigationDesign.SINGLE_DESIGN_MARCH_2015.toString() + ".");
			}
			if(maxMismatchAdapter < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in adapters for " + LigationDesign.SINGLE_DESIGN_MARCH_2015.toString() + ".");
			}
			break;
		case SINGLE_DESIGN_MAY_2015:
			 if(may2015firstBarcodeFile == null) {
				 throw new IllegalArgumentException("Must provide file of initial barcode equivalence classesfor " + LigationDesign.SINGLE_DESIGN_MAY_2015.toString() + ".");
			 }
			 if(may2015OddBarcodeFile == null || may2015evenBarcodeFile == null) {
				 throw new IllegalArgumentException("Must provide file of odd and even barcode equivalence classes for " + LigationDesign.SINGLE_DESIGN_MAY_2015.toString() + ".");				 
			 }
			 if(may2015lastBarcodeFile == null) {
				 throw new IllegalArgumentException("Must provide file of final barcode equivalence classes for " + LigationDesign.SINGLE_DESIGN_MAY_2015.toString() + ".");				 
			 }
			 if(readLength < 1) {
				 throw new IllegalArgumentException("Must provide read length for " + LigationDesign.SINGLE_DESIGN_MAY_2015.toString() + ".");				 
			 }
			 if(may2015maxMismatchFirstBarcode < 0) {
				 throw new IllegalArgumentException("Must provide max mismatches in initial barcode for " + LigationDesign.SINGLE_DESIGN_MAY_2015.toString() + ".");				 
			 }
			 if(may2015maxMismatchLastBarcode < 0) {
				 throw new IllegalArgumentException("Must provide max mismatches in final barcode for " + LigationDesign.SINGLE_DESIGN_MAY_2015.toString() + ".");				 
			 }
			 if(may2015maxMismatchOddEvenBarcode < 0) {
				 throw new IllegalArgumentException("Must provide max mismatches in odd/even barcode for " + LigationDesign.SINGLE_DESIGN_MAY_2015.toString() + ".");				 
			 }
			 break;
		case PAIRED_DESIGN_JANUARY_2016:
			if(oddBarcodeList == null) {
				throw new IllegalArgumentException("Must provide odd barcode list (for read 2) for " + LigationDesign.PAIRED_DESIGN_JANUARY_2016.toString() + ".");
			}
			if(evenBarcodeList == null) {
				throw new IllegalArgumentException("Must provide even barcode list (for read 2) for " + LigationDesign.PAIRED_DESIGN_JANUARY_2016.toString() + ".");
			}
			if(yShapeBarcodeList == null) {
				throw new IllegalArgumentException("Must provide Y shape barcode list (for read 2) for " + LigationDesign.PAIRED_DESIGN_JANUARY_2016.toString() + ".");
			}
			if(read2Length < 1) {
				throw new IllegalArgumentException("Must provide read 2 length for " + LigationDesign.PAIRED_DESIGN_JANUARY_2016.toString() + ".");
			}
			if(maxMismatchBarcodeRead2 < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in barcode in read 2 for " + LigationDesign.PAIRED_DESIGN_JANUARY_2016.toString() + ".");
			}
			if(maxMismatchYShapeRead2 < 0) {
				throw new IllegalArgumentException("Must provide max mismatches in Y shape barcode in read 2 for " + LigationDesign.PAIRED_DESIGN_JANUARY_2016.toString() + ".");
			}
			break;
		default:
			throw new UnsupportedOperationException("Not implemented for " + design.toString());		
		}
	}
	
	private static String debugOption = "-d";
	private static String outPrefixOption = "-ob3d";
	private static String emailOption = "-e";
	private static String fastqOption = "-fq";
	private static String fastq1Option = "-fq1";
	private static String fastq2Option = "-fq2";
	private static String readLengthOption = "-rl";
	private static String read1LengthOption = "-r1l";
	private static String read2LengthOption = "-r2l";
	private static String oddBarcodeTableOption = "-ob";
	private static String evenBarcodeTableOption = "-eb";
	private static String yShapeBarcodeTableOption = "-yb";
	private static String rpmOption = "-rpm";
	private static String dpmOption = "-dpm";
	private static String maxMismatchBarcodeOption = "-mmb";
	private static String maxMismatchBarcodeRead1Option = "-mmbr1";
	private static String maxMismatchEvenOddBarcodeRead2Option = "-mmeor2";
	private static String maxMismatchYShapeBarcodeRead2Option = "-mmyr2";
	private static String maxMismatchRpmOption = "-mmr";
	private static String maxMismatchDpmOption = "-mmd";
	private static String maxMismatchAdapterOption = "-mma";
	private static String enforceOddEvenOption = "-oe";
	private static String countBarcodesOption = "-cb";
	private static String verboseOption = "-v";
	private static String batchOption = "-bt";
	private static String jarOption = "-j";
	private static String numFastqOption = "-nf";
	private static String designNameOption = "-l";
	private static String totalNumBarcodesOption = "-nb";
	private static String splitOutFilesBySwitchesOption = "-ss";
	private static String suffixFastqOption = "-wsf";
	private static String suffixFastq1Option = "-wsf1";
	private static String suffixFastq2Option = "-wsf2";
	private static String getLastBarcodesOption = "-lb";
	private static String lastNumBarcodesOption = "-nlb";
	private static String adapterSeqFastaOption = "-afa";
	private static String may2015firstBarcodeFileOption = "-fbecf";
	private static String may2015oddBarcodeFileOption = "-oecf";
	private static String may2015evenBarcodeFileOption = "-eecf";
	private static String may2015lastBarcodeFileOption = "-lbecf";
	private static String may2015maxMismatchFirstBarcodeOption = "-mmfb";
	private static String may2015maxMismatchLastBarcodeOption = "-mmlb";
	private static String may2015maxMismatchOddEvenBarcodeOption = "-mmboe";
	private static String july2015read1BarcodeFileOption = "-r1b";
	private static String totalNumBarcodesRead2Option = "-nbr2";
	private static String rpmOrDpmOption = "-rpmordpm";
	private static String maxMismatchRpmOrDpmOption = "-mmrord";
	
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
		
		CommandLineParser p = new CommandLineParser();
		p.addBooleanArg(debugOption, "Debug logging on", false, false);
		p.addStringArg(outPrefixOption, "Output file prefix for barcode identification", false, null);
		p.addStringArg(fastqOption, "Fastq file if only identifying barcodes in one mate", false, null);
		p.addStringArg(fastq1Option, "Fastq file read 1 if identifying barcodes in both mates", false, null);
		p.addStringArg(fastq2Option, "Fastq file read 2 if identifying barcodes in both mates", false, null);
		p.addIntArg(readLengthOption, "Read length if only identifying barcodes in one mate", false, -1);
		p.addIntArg(read1LengthOption, "Read 1 length if identifying barcodes in both mates", false, -1);
		p.addIntArg(read2LengthOption, "Read 2 length if identifying barcodes in both mates", false, -1);
		p.addStringArg(oddBarcodeTableOption, "Odd barcode table file (format: barcode_id	barcode_seq)", false, null);
		p.addStringArg(evenBarcodeTableOption, "Even barcode table file (format: barcode_id	barcode_seq)", false, null);
		p.addStringArg(yShapeBarcodeTableOption, "Y shape barcode table file (format: barcode_id	barcode_seq)", false, null);
		p.addStringArg(dpmOption, "Single fixed DPM sequence", false, null);
		p.addStringArg(rpmOption, "Single fixed RPM sequence", false, null);
		p.addIntArg(maxMismatchBarcodeOption, "Max mismatches in barcode if only identifying barcodes in one mate", false, -1);
		p.addIntArg(maxMismatchBarcodeRead1Option, "Max mismatches in barcode in read 1 if identifying barcodes in both mates", false, -1);
		p.addIntArg(maxMismatchEvenOddBarcodeRead2Option, "Max mismatches in even/odd barcode in read 2 if identifying barcodes in both mates", false, -1);
		p.addIntArg(maxMismatchYShapeBarcodeRead2Option, "Max mismatches in Y shape barcode in read 2 if identifying barcodes in both mates", false, -1);
		p.addIntArg(maxMismatchRpmOption, "Max mismatches in RPM", false, -1);
		p.addIntArg(maxMismatchDpmOption, "Max mismatches in DPM", false, -1);
		p.addIntArg(maxMismatchAdapterOption, "Max mismatches in adapter for single design from March 2015 with multiple adapters between barcodes and read", false, -1);
		p.addBooleanArg(enforceOddEvenOption, "Enforce odd/even alternation for barcodes", false, false);
		p.addBooleanArg(countBarcodesOption, "Count barcodes and write to table", false, false);
		p.addBooleanArg(verboseOption, "Use verbose output in barcode identification table", false, false);
		p.addBooleanArg(batchOption, "Batch out writing of barcode table", false, false);
		p.addStringArg(jarOption, "This jar file for batched jobs", false, null);
		p.addIntArg(numFastqOption, "Number of fastq files to divide into if batching", false, 20);
		p.addStringArg(emailOption, "Email address for OGS jobs", false, null);
		p.addStringArg(designNameOption, "Ligation design. Options: " + LigationDesign.getNamesAsCommaSeparatedList(), true);
		p.addIntArg(totalNumBarcodesOption, "Total number of barcode ligations if only identifying barcodes in one mate", false, -1);
		p.addIntArg(totalNumBarcodesRead2Option, "Total number of barcode ligations on read 2 for paired design with barcodes in both mates", false, -1);
		p.addBooleanArg(splitOutFilesBySwitchesOption, "Split output files by switch values in reads", false, false);
		p.addStringArg(suffixFastqOption, "Also write fastq file of the part of each read after the last matched layout element, if only identifying barcodes in one mate", false, null);
		p.addStringArg(suffixFastq1Option, "Also write fastq file of the part of each read 1 after the last matched layout element, if identifying barcodes in both mates", false, null);
		p.addStringArg(suffixFastq2Option, "Also write fastq file of the part of each read 2 after the last matched layout element, if identifying barcodes in both mates", false, null);
		p.addBooleanArg(getLastBarcodesOption, "Also get the last N barcodes from each fragment and write as a column in table", false, false);
		p.addIntArg(lastNumBarcodesOption, "Number of last barcodes to get for " + getLastBarcodesOption + " option", false, -1);
		p.addStringArg(adapterSeqFastaOption, "Fasta file of adapter sequences for single design from March 2015 with multiple adapters between barcodes and read", false, null);
		p.addStringArg(may2015firstBarcodeFileOption, "Table of initial barcode equivalence classes for single design from May 2015 (format: equiv_class	barcode_id	barcode_seq)", false, null);
		p.addStringArg(may2015lastBarcodeFileOption, "Table of final barcode equivalence classes for single design from May 2015 (format: equiv_class	barcode_id	barcode_seq)", false, null);
		p.addStringArg(may2015oddBarcodeFileOption, "Table of odd barcode equivalence classes for single design from May 2015 (format: equiv_class	barcode_id	barcode_seq)", false, null);
		p.addStringArg(may2015evenBarcodeFileOption, "Table of even barcode equivalence classes for single design from May 2015 (format: equiv_class	barcode_id	barcode_seq)", false, null);
		p.addIntArg(may2015maxMismatchFirstBarcodeOption, "Max mismatches in initial barcode for single design from May 2015", false, -1);
		p.addIntArg(may2015maxMismatchLastBarcodeOption, "Max mismatches in final barcode for single design from May 2015", false, -1);
		p.addIntArg(may2015maxMismatchOddEvenBarcodeOption, "Max mismatches in odd/even barcode for single design from May 2015", false, -1);
		p.addStringArg(july2015read1BarcodeFileOption, "Table of read1 barcodes for paired design from July 2015 (format: barcode_id	barcode_seq)", false, null);
		p.addStringArg(rpmOrDpmOption, "RPM or DPM sequence for December 2015 design (depending on whether these are RNA or DNA reads", false, null);
		p.addIntArg(maxMismatchRpmOrDpmOption, "Max mismatches in RPM or DPM for December 2015 design (depending on whether these are RNA or DNA reads", false, -1);
		p.parse(args);
		if(p.getBooleanArg(debugOption)) {
			ReadLayout.logger.setLevel(Level.DEBUG);
			BarcodeSet.logger.setLevel(Level.DEBUG);
			ReadLayoutFactory.logger.setLevel(Level.DEBUG);
			BarcodeSequence.logger.setLevel(Level.DEBUG);
			Barcode.logger.setLevel(Level.DEBUG);
			BarcodedFragmentImpl.logger.setLevel(Level.DEBUG);
			FixedSequence.logger.setLevel(Level.DEBUG);
			BarcodedDNAFragment.logger.setLevel(Level.DEBUG);
			BarcodedRNAFragment.logger.setLevel(Level.DEBUG);
			AnySequence.logger.setLevel(Level.DEBUG);
			BarcodeAnalysis.logger.setLevel(Level.DEBUG);
			GenericElementMatcher.logger.setLevel(Level.DEBUG);
			BitapMatcher.logger.setLevel(Level.DEBUG);
			HashMatcher.logger.setLevel(Level.DEBUG);
			ReadLayoutSequenceHash.logger.setLevel(Level.DEBUG);
		}
		
		String outPrefix = p.getStringArg(outPrefixOption);
		String fastq = p.getStringArg(fastqOption);
		String fastq1 = p.getStringArg(fastq1Option);
		String fastq2 = p.getStringArg(fastq2Option);
		int readLength = p.getIntArg(readLengthOption);
		int read1Length = p.getIntArg(read1LengthOption);
		int read2Length = p.getIntArg(read2LengthOption);
		String oddBarcodeList = p.getStringArg(oddBarcodeTableOption);
		String evenBarcodeList = p.getStringArg(evenBarcodeTableOption);
		String yShapeBarcodeList = p.getStringArg(yShapeBarcodeTableOption);
		String rpm = p.getStringArg(rpmOption);
		String dpm = p.getStringArg(dpmOption);
		MAX_MISMATCH_BARCODE = p.getIntArg(maxMismatchBarcodeOption);
		MAX_MISMATCH_BARCODE_READ1 = p.getIntArg(maxMismatchBarcodeRead1Option);
		MAX_MISMATCH_EVEN_ODD_BARCODE_READ2 = p.getIntArg(maxMismatchEvenOddBarcodeRead2Option);
		MAX_MISMATCH_Y_SHAPE_BARCODE_READ2 = p.getIntArg(maxMismatchYShapeBarcodeRead2Option);
		MAX_MISMATCH_RPM = p.getIntArg(maxMismatchRpmOption);
		MAX_MISMATCH_DPM = p.getIntArg(maxMismatchDpmOption);
		boolean enforceOddEven = p.getBooleanArg(enforceOddEvenOption);
		boolean countBarcodes = p.getBooleanArg(countBarcodesOption);
		boolean verbose = p.getBooleanArg(verboseOption);
		boolean batch = p.getBooleanArg(batchOption);
		String designName = p.getStringArg(designNameOption);
		LigationDesign design = LigationDesign.fromString(designName);
		int totalNumBarcodes = p.getIntArg(totalNumBarcodesOption);
		boolean splitOutputBySwitchesInLayout = p.getBooleanArg(splitOutFilesBySwitchesOption);
		String suffixFastq = p.getStringArg(suffixFastqOption);
		String suffixFastq1 = p.getStringArg(suffixFastq1Option);
		String suffixFastq2 = p.getStringArg(suffixFastq2Option);
		GET_LAST_BARCODES = p.getBooleanArg(getLastBarcodesOption);
		int lastNumBarcodes = p.getIntArg(lastNumBarcodesOption);
		String adapterSeqFasta = p.getStringArg(adapterSeqFastaOption);
		MAX_MISMATCH_ADAPTER = p.getIntArg(maxMismatchAdapterOption);
		if(lastNumBarcodes > 0) {
			NUM_LAST_BARCODES = lastNumBarcodes;
		}
		String may2015firstBarcodeFile = p.getStringArg(may2015firstBarcodeFileOption);
		String may2015OddBarcodeFile = p.getStringArg(may2015oddBarcodeFileOption);
		String may2015EvenBarcodeFile = p.getStringArg(may2015evenBarcodeFileOption);
		String may2015lastBarcodeFile = p.getStringArg(may2015lastBarcodeFileOption);
		int may2015maxMismatchFirstBarcode = p.getIntArg(may2015maxMismatchFirstBarcodeOption);
		int may2015maxMismatchLastBarcode = p.getIntArg(may2015maxMismatchLastBarcodeOption);
		int may2015maxMismatchOddEvenBarcode = p.getIntArg(may2015maxMismatchOddEvenBarcodeOption);
		String july2015firstBarcodeFile = p.getStringArg(july2015read1BarcodeFileOption);
		int totalNumBarcodesRead2 = p.getIntArg(totalNumBarcodesRead2Option);		
		
		validateCommandLine(p);
		
		// Make table of reads IDs and barcodes
		if(outPrefix != null) {
			if(batch) {
				if(fastq != null) divideFastqAndFindBarcodesSingleFastq(p);
				if(fastq1 != null && fastq2 != null) divideFastqAndFindBarcodesPairedFastq(p);
			} else {
				switch(design) {
				case PAIRED_DESIGN_BARCODE_IN_READ2:
					BarcodedReadLayout layout1 = ReadLayoutFactory.getRead2LayoutRnaDna3DPairedDesign(evenBarcodeList, oddBarcodeList, totalNumBarcodes, rpm, 
							readLength, MAX_MISMATCH_BARCODE, MAX_MISMATCH_RPM, enforceOddEven);
					findBarcodes(fastq, layout1, outPrefix, verbose, splitOutputBySwitchesInLayout, suffixFastq);
					break;
				case SINGLE_DESIGN_WITH_SWITCH:
					BarcodedReadLayout layout2 = ReadLayoutFactory.getReadLayoutRnaDna3DSingleDesignWithRnaDnaSwitch(evenBarcodeList, oddBarcodeList, totalNumBarcodes, 
							rpm, dpm, readLength, MAX_MISMATCH_BARCODE, MAX_MISMATCH_RPM, MAX_MISMATCH_DPM, enforceOddEven);
					findBarcodes(fastq, layout2, outPrefix, verbose, splitOutputBySwitchesInLayout, suffixFastq);
					break;
				case SINGLE_DESIGN:
					BarcodedReadLayout layout3 = ReadLayoutFactory.getReadLayoutRnaDna3DSingleDesign(evenBarcodeList, oddBarcodeList, totalNumBarcodes, dpm, readLength, 
							MAX_MISMATCH_BARCODE, MAX_MISMATCH_DPM, enforceOddEven);
					findBarcodes(fastq, layout3, outPrefix, verbose, splitOutputBySwitchesInLayout, suffixFastq);
					break;
				case SINGLE_DESIGN_MARCH_2015:
					BarcodedReadLayout layout4 = ReadLayoutFactory.getReadLayoutRnaDna3DSingleDesignMarch2015(evenBarcodeList, oddBarcodeList, totalNumBarcodes, adapterSeqFasta, 
							readLength, MAX_MISMATCH_BARCODE, MAX_MISMATCH_ADAPTER, enforceOddEven);
					findBarcodes(fastq, layout4, outPrefix, verbose, splitOutputBySwitchesInLayout, suffixFastq);
					break;
				case SINGLE_DESIGN_MAY_2015:
					BarcodedReadLayout layout5 = ReadLayoutFactory.getReadLayoutRnaDna3DSingleDesignMay2015(may2015firstBarcodeFile, may2015OddBarcodeFile, may2015EvenBarcodeFile, 
							may2015lastBarcodeFile, readLength, may2015maxMismatchFirstBarcode, may2015maxMismatchOddEvenBarcode, may2015maxMismatchLastBarcode);
					findBarcodes(fastq, layout5, outPrefix, verbose, splitOutputBySwitchesInLayout, suffixFastq);
					break;
				case PAIRED_DESIGN_JULY_2015:
					BarcodedReadLayout layout6read1 = ReadLayoutFactory.getRead1LayoutRnaDna3DPairedDesignJuly2015(july2015firstBarcodeFile, MAX_MISMATCH_BARCODE_READ1, read1Length);
					BarcodedReadLayout layout6read2 = ReadLayoutFactory.getRead2LayoutRnaDna3DPairedDesignJuly2015(oddBarcodeList, evenBarcodeList, yShapeBarcodeList, totalNumBarcodesRead2, 
							MAX_MISMATCH_EVEN_ODD_BARCODE_READ2, MAX_MISMATCH_Y_SHAPE_BARCODE_READ2, read2Length, enforceOddEven);
					findBarcodes(fastq1, fastq2, layout6read1, layout6read2, outPrefix, suffixFastq1, suffixFastq2, verbose);
					break;
				case PAIRED_DESIGN_JANUARY_2016:
					BarcodedReadLayout layout7read2 = ReadLayoutFactory.getRead2LayoutRnaDna3DPairedDesignJanuary2016(evenBarcodeList, oddBarcodeList, 
							yShapeBarcodeList, MAX_MISMATCH_EVEN_ODD_BARCODE_READ2, MAX_MISMATCH_Y_SHAPE_BARCODE_READ2, read2Length);
					findBarcodes(fastq, layout7read2, outPrefix, verbose, false);
					break;
				default:
					throw new IllegalArgumentException("Not implemented");
				}
			}
		}

		// Make histogram of barcodes
		if(countBarcodes) {
			switch(design) {
			case PAIRED_DESIGN_BARCODE_IN_READ2:
				BarcodedReadLayout layout1 = ReadLayoutFactory.getRead2LayoutRnaDna3DPairedDesign(evenBarcodeList, oddBarcodeList, totalNumBarcodes, rpm, readLength, MAX_MISMATCH_BARCODE, 
						MAX_MISMATCH_RPM, enforceOddEven);
				countBarcodes(fastq, layout1);
				break;
			case SINGLE_DESIGN_WITH_SWITCH:
				BarcodedReadLayout layout2 = ReadLayoutFactory.getReadLayoutRnaDna3DSingleDesignWithRnaDnaSwitch(evenBarcodeList, oddBarcodeList, totalNumBarcodes, rpm, dpm, readLength, 
						MAX_MISMATCH_BARCODE, MAX_MISMATCH_RPM, MAX_MISMATCH_DPM, enforceOddEven);
				countBarcodes(fastq, layout2);
				break;
			default:
				throw new IllegalArgumentException("Not implemented");			
			}
		}
			
		logger.info("");
		logger.info("All done.");
				
	}

}
