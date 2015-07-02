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
import matcher.GenericElementMatcher;
import nextgen.core.pipeline.util.OGSUtils;
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
	
	/**
	 * For RNA-DNA 3D barcoding method
	 * Identify barcodes in reads and write to a table
	 * Split the fastq file into several smaller files and batch out the writing of the table
	 * @param p CommandLineParser object containing all the options
	 * @throws IOException
	 * @throws DrmaaException
	 * @throws InterruptedException
	 */
	private static void divideFastqAndFindBarcodes(CommandLineParser p) throws IOException, DrmaaException, InterruptedException {
		
		String outPrefix = p.getStringArg(outPrefixOption);
		String email = p.getStringArg(emailOption);
		String fastq = p.getStringArg(fastqOption);
		int readLength = p.getIntArg(readLengthOption);
		String oddBarcodeList = p.getStringArg(oddBarcodeListOption);
		String evenBarcodeList = p.getStringArg(evenBarcodeListOption);
		String rpm = p.getStringArg(rpmOption);
		String dpm = p.getStringArg(dpmOption);
		int maxMismatchBarcode = p.getIntArg(maxMismatchBarcodeOption);
		int maxMismatchRpm = p.getIntArg(maxMismatchRpmOption);
		int maxMismatchDpm = p.getIntArg(maxMismatchDpmOption);
		int maxMismatchAdapter = p.getIntArg(maxMismatchAdapterOption);
		boolean enforceOddEven = p.getBooleanArg(enforceOddEvenOption);
		boolean verbose = p.getBooleanArg(verboseOption);
		String jar = p.getStringArg(jarOption);
		int numFastq = p.getIntArg(numFastqOption);
		String designName = p.getStringArg(designNameOption);
		int totalNumBarcodes = p.getIntArg(totalNumBarcodesOption);
		boolean splitOutputBySwitchesInLayout = p.getBooleanArg(splitOutFilesBySwitchesOption);
		String suffixFastq = p.getStringArg(suffixFastqOption);
		boolean getLastBarcodes = p.getBooleanArg(getLastBarcodesOption);
		int lastNumBarcodes = p.getIntArg(lastNumBarcodesOption);
		String adapterSeqFasta = p.getStringArg(adapterSeqFastaOption);
		String may2015firstBarcodeFile = p.getStringArg(may2015firstBarcodeFileOption);
		String may2015oddBarcodeFile = p.getStringArg(may2015oddBarcodeFileOption);
		String may2015evenBarcodeFile = p.getStringArg(may2015evenBarcodeFileOption);
		String may2015lastBarcodeFile = p.getStringArg(may2015lastBarcodeFileOption);
		int may2015maxMismatchFirstBarcode = p.getIntArg(may2015maxMismatchFirstBarcodeOption);
		int may2015maxMismatchLastBarcode = p.getIntArg(may2015maxMismatchLastBarcodeOption);
		int may2015maxMismatchOddEvenBarcode = p.getIntArg(may2015maxMismatchOddEvenBarcodeOption);
		
		Collection<String> splitFastqs = FastqUtils.divideFastqFile(fastq, numFastq);
		Map<String, String> splitTables = new TreeMap<String, String>();
		int i = 0;
		for(String fq : splitFastqs) {
			splitTables.put(fq, outPrefix + "." + i); // Create split table names and index by associated input fastq
			i++;
		}
		Collection<String> splitSuffixFastqs = new ArrayList<String>();
		for(String fq : splitFastqs) {
			splitSuffixFastqs.add(makeSuffixFastqName(fq));
		}
		Collection<Job> jobs = new ArrayList<Job>();
		for(String fq : splitTables.keySet()) {
			String cmmd = "java -jar -Xmx25g -Xms15g -Xmn10g " + jar;
			cmmd += " " + outPrefixOption + " " + splitTables.get(fq);
			cmmd += " " + fastqOption + " " + fq;
			cmmd += " " + readLengthOption + " " + readLength;
			cmmd += " " + oddBarcodeListOption + " " + oddBarcodeList;
			cmmd += " " + evenBarcodeListOption + " " + evenBarcodeList;
			cmmd += " " + rpmOption + " " + rpm;
			cmmd += " " + dpmOption + " " + dpm;
			cmmd += " " + maxMismatchBarcodeOption + " " + maxMismatchBarcode;
			cmmd += " " + maxMismatchRpmOption + " " + maxMismatchRpm;
			cmmd += " " + maxMismatchDpmOption + " " + maxMismatchDpm;
			cmmd += " " + maxMismatchAdapterOption + " " + maxMismatchAdapter;
			cmmd += " " + enforceOddEvenOption + " " + enforceOddEven;
			cmmd += " " + verboseOption + " " + verbose;
			cmmd += " " + designNameOption + " " + designName;
			cmmd += " " + totalNumBarcodesOption + " " + totalNumBarcodes;
			cmmd += " " + splitOutFilesBySwitchesOption + " " + splitOutputBySwitchesInLayout;
			cmmd += " " + suffixFastqOption + " " + makeSuffixFastqName(fq);
			cmmd += " " + getLastBarcodesOption + " " + getLastBarcodes;
			cmmd += " " + lastNumBarcodesOption + " " + lastNumBarcodes;
			cmmd += " " + adapterSeqFastaOption + " " + adapterSeqFasta;
			cmmd += " " + may2015firstBarcodeFileOption + " " + may2015firstBarcodeFile;
			cmmd += " " + may2015oddBarcodeFileOption + " " + may2015oddBarcodeFile;
			cmmd += " " + may2015evenBarcodeFileOption + " " + may2015evenBarcodeFile;
			cmmd += " " + may2015lastBarcodeFileOption + " " + may2015lastBarcodeFile;
			cmmd += " " + may2015maxMismatchFirstBarcodeOption + " " + may2015maxMismatchFirstBarcode;
			cmmd += " " + may2015maxMismatchLastBarcodeOption + " " + may2015maxMismatchLastBarcode;
			cmmd += " " + may2015maxMismatchOddEvenBarcodeOption + " " + may2015maxMismatchOddEvenBarcode;
			String jobNameWithSlashes = "OGS_job_" + fq;
			String jobName = jobNameWithSlashes.replaceAll("/", "_");
			OGSJob job = new OGSJob(drmaaSession, cmmd, true, jobName, email);
			job.submit();
			jobs.add(job);
		}
		JobUtils.waitForAll(jobs);
		// Only try to concatenate the files if they were not split by switches
		if(!splitOutputBySwitchesInLayout) {
			
			FileWriter tableWriter = new FileWriter(outPrefix);
			for(String fq : splitTables.keySet()) {
				String table = splitTables.get(fq);
				FileReader r = new FileReader(table);
				BufferedReader b = new BufferedReader(r);
				while(b.ready()) {
					tableWriter.write(b.readLine() + "\n");
				}
				r.close();
				b.close();
				File f = new File(table);
				f.delete();
			}
			tableWriter.close();

			if(suffixFastq != null) {
				FileWriter fastqWriter = new FileWriter(suffixFastq);
				for(String fq : splitSuffixFastqs) {
					FileReader r = new FileReader(fq);
					BufferedReader b = new BufferedReader(r);
					while(b.ready()) {
						fastqWriter.write(b.readLine() + "\n");
					}
					r.close();
					b.close();
					File f = new File(fq);
					f.delete();
				}
				fastqWriter.close();
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
	@SuppressWarnings("unused")
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
			GenericElementMatcher matcher = new GenericElementMatcher(layout, seq);
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
		FastqParser iter = new FastqParser();
		iter.start(new File(fastq));
		int numDone = 0;
		while(iter.hasNext()) {
			FastqSequence record = iter.next();
			String seq = record.getSequence();
			String name = record.getName();
			if(new GenericElementMatcher(layout, seq).getMatchedElements() != null) {
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
		int readLength = commandLineParser.getIntArg(readLengthOption);
		String oddBarcodeList = commandLineParser.getStringArg(oddBarcodeListOption);
		String evenBarcodeList = commandLineParser.getStringArg(evenBarcodeListOption);
		String rpm = commandLineParser.getStringArg(rpmOption);
		String dpm = commandLineParser.getStringArg(dpmOption);
		int maxMismatchBarcode = commandLineParser.getIntArg(maxMismatchBarcodeOption);
		int maxMismatchRpm = commandLineParser.getIntArg(maxMismatchRpmOption);
		int maxMismatchDpm = commandLineParser.getIntArg(maxMismatchDpmOption);
		boolean countBarcodes = commandLineParser.getBooleanArg(countBarcodesOption);
		boolean batch = commandLineParser.getBooleanArg(batchOption);
		String jar = commandLineParser.getStringArg(jarOption);
		int numFastq = commandLineParser.getIntArg(numFastqOption);
		String designName = commandLineParser.getStringArg(designNameOption);
		LigationDesign design = LigationDesign.fromString(designName);
		int totalNumBarcodes = commandLineParser.getIntArg(totalNumBarcodesOption);		
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


		if((outPrefix != null || countBarcodes) && fastq == null) {
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
		default:
			throw new UnsupportedOperationException("Not implemented for " + design.toString());		
		}
	}
	
	private static String debugOption = "-d";
	private static String outPrefixOption = "-ob3d";
	private static String emailOption = "-e";
	private static String fastqOption = "-fq";
	private static String readLengthOption = "-rl";
	private static String oddBarcodeListOption = "-ob";
	private static String evenBarcodeListOption = "-eb";
	private static String rpmOption = "-rpm";
	private static String dpmOption = "-dpm";
	private static String maxMismatchBarcodeOption = "-mmb";
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
		p.addStringArg(fastqOption, "Fastq file", false, null);
		p.addIntArg(readLengthOption, "Read length", false, -1);
		p.addStringArg(oddBarcodeListOption, "Odd barcode table file (format: barcode_id	barcode_seq)", false, null);
		p.addStringArg(evenBarcodeListOption, "Even barcode table file (format: barcode_id	barcode_seq)", false, null);
		p.addStringArg(dpmOption, "DPM sequence", false, null);
		p.addStringArg(rpmOption, "RPM sequence", false, null);
		p.addIntArg(maxMismatchBarcodeOption, "Max mismatches in barcode", false, -1);
		p.addIntArg(maxMismatchRpmOption, "Max mismatches in RPM", false, -1);
		p.addIntArg(maxMismatchDpmOption, "Max mismatches in DPM", false, -1);
		p.addIntArg(maxMismatchAdapterOption, "Max mismatches in adapter for single design from March 2015 with multiple adapters between barcodes and read", false, -1);
		p.addBooleanArg(enforceOddEvenOption, "Enforce odd/even alternation for barcodes", false, false);
		p.addBooleanArg(countBarcodesOption, "Count barcodes", false, false);
		p.addBooleanArg(verboseOption, "Verbose output table for barcode identification", false, false);
		p.addBooleanArg(batchOption, "Batch out writing of barcode table", false, false);
		p.addStringArg(jarOption, "This jar file for batched jobs", false, null);
		p.addIntArg(numFastqOption, "Number of fastq files to divide into if batching", false, 20);
		p.addStringArg(emailOption, "Email address for OGS jobs", false, null);
		p.addStringArg(designNameOption, "Ligation design. Options: " + LigationDesign.getNamesAsCommaSeparatedList(), true);
		p.addIntArg(totalNumBarcodesOption, "Total number of barcode ligations", false, -1);
		p.addBooleanArg(splitOutFilesBySwitchesOption, "Split output files by switch values in reads", false, false);
		p.addStringArg(suffixFastqOption, "Also write fastq file of the part of each read after the last matched layout element", false, null);
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
		}
		
		String outPrefix = p.getStringArg(outPrefixOption);
		String fastq = p.getStringArg(fastqOption);
		int readLength = p.getIntArg(readLengthOption);
		String oddBarcodeList = p.getStringArg(oddBarcodeListOption);
		String evenBarcodeList = p.getStringArg(evenBarcodeListOption);
		String rpm = p.getStringArg(rpmOption);
		String dpm = p.getStringArg(dpmOption);
		int maxMismatchBarcode = p.getIntArg(maxMismatchBarcodeOption);
		int maxMismatchRpm = p.getIntArg(maxMismatchRpmOption);
		int maxMismatchDpm = p.getIntArg(maxMismatchDpmOption);
		boolean enforceOddEven = p.getBooleanArg(enforceOddEvenOption);
		boolean countBarcodes = p.getBooleanArg(countBarcodesOption);
		boolean verbose = p.getBooleanArg(verboseOption);
		boolean batch = p.getBooleanArg(batchOption);
		String designName = p.getStringArg(designNameOption);
		LigationDesign design = LigationDesign.fromString(designName);
		int totalNumBarcodes = p.getIntArg(totalNumBarcodesOption);
		boolean splitOutputBySwitchesInLayout = p.getBooleanArg(splitOutFilesBySwitchesOption);
		String suffixFastq = p.getStringArg(suffixFastqOption);
		GET_LAST_BARCODES = p.getBooleanArg(getLastBarcodesOption);
		int lastNumBarcodes = p.getIntArg(lastNumBarcodesOption);
		String adapterSeqFasta = p.getStringArg(adapterSeqFastaOption);
		int maxMismatchAdapter = p.getIntArg(maxMismatchAdapterOption);
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
		
		validateCommandLine(p);
		
		// Make table of reads IDs and barcodes
		if(outPrefix != null) {
			if(batch) {
				divideFastqAndFindBarcodes(p);
			} else {
				switch(design) {
				case PAIRED_DESIGN_BARCODE_IN_READ2:
					BarcodedReadLayout layout1 = ReadLayoutFactory.getRead2LayoutRnaDna3DPairedDesign(evenBarcodeList, oddBarcodeList, totalNumBarcodes, rpm, readLength, maxMismatchBarcode, maxMismatchRpm, enforceOddEven);
					findBarcodes(fastq, layout1, outPrefix, verbose, splitOutputBySwitchesInLayout, suffixFastq);
					break;
				case SINGLE_DESIGN_WITH_SWITCH:
					BarcodedReadLayout layout2 = ReadLayoutFactory.getReadLayoutRnaDna3DSingleDesignWithRnaDnaSwitch(evenBarcodeList, oddBarcodeList, totalNumBarcodes, rpm, dpm, readLength, maxMismatchBarcode, maxMismatchRpm, maxMismatchDpm, enforceOddEven);
					findBarcodes(fastq, layout2, outPrefix, verbose, splitOutputBySwitchesInLayout, suffixFastq);
					break;
				case SINGLE_DESIGN:
					BarcodedReadLayout layout3 = ReadLayoutFactory.getReadLayoutRnaDna3DSingleDesign(evenBarcodeList, oddBarcodeList, totalNumBarcodes, dpm, readLength, maxMismatchBarcode, maxMismatchDpm, enforceOddEven);
					findBarcodes(fastq, layout3, outPrefix, verbose, splitOutputBySwitchesInLayout, suffixFastq);
					break;
				case SINGLE_DESIGN_MARCH_2015:
					BarcodedReadLayout layout4 = ReadLayoutFactory.getReadLayoutRnaDna3DSingleDesignMarch2015(evenBarcodeList, oddBarcodeList, totalNumBarcodes, adapterSeqFasta, readLength, maxMismatchBarcode, maxMismatchAdapter, enforceOddEven);
					findBarcodes(fastq, layout4, outPrefix, verbose, splitOutputBySwitchesInLayout, suffixFastq);
					break;
				case SINGLE_DESIGN_MAY_2015:
					BarcodedReadLayout layout5 = ReadLayoutFactory.getReadLayoutRnaDna3DSingleDesignMay2015(may2015firstBarcodeFile, may2015OddBarcodeFile, may2015EvenBarcodeFile, may2015lastBarcodeFile, readLength, may2015maxMismatchFirstBarcode, may2015maxMismatchOddEvenBarcode, may2015maxMismatchLastBarcode);
					findBarcodes(fastq, layout5, outPrefix, verbose, splitOutputBySwitchesInLayout, suffixFastq);
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
				BarcodedReadLayout layout1 = ReadLayoutFactory.getRead2LayoutRnaDna3DPairedDesign(evenBarcodeList, oddBarcodeList, totalNumBarcodes, rpm, readLength, maxMismatchBarcode, maxMismatchRpm, enforceOddEven);
				countBarcodes(fastq, layout1);
				break;
			case SINGLE_DESIGN_WITH_SWITCH:
				BarcodedReadLayout layout2 = ReadLayoutFactory.getReadLayoutRnaDna3DSingleDesignWithRnaDnaSwitch(evenBarcodeList, oddBarcodeList, totalNumBarcodes, rpm, dpm, readLength, maxMismatchBarcode, maxMismatchRpm, maxMismatchDpm, enforceOddEven);
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
