package programs.barcode;

import fragment.BarcodedFragmentWithSwitches;
import fragment.BasicBarcodedFragment;
import guttmanlab.core.pipeline.util.FastqParser;
import guttmanlab.core.pipeline.util.FastqSequence;
import guttmanlab.core.util.CommandLineParser;
import guttmanlab.core.util.StringParser;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import matcher.BitapMatcher;
import matcher.GenericElementMatcher;
import matcher.HashMatcher;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.ggf.drmaa.DrmaaException;

import readelement.AnySequence;
import readelement.BarcodeSet;
import readelement.FixedSequence;
import readelement.FragmentBarcode;
import readelement.ReadSequenceElement;
import readelement.Switch;
import readlayout.BarcodedReadLayout;
import readlayout.ReadLayout;
import readlayout.ReadLayoutSequenceHash;
import util.BarcodeAnalysisConfigFile;
import contact.BarcodeSequence;

/**
 * Barcode identification and other analyses
 * @author prussell
 *
 */
public final class BarcodeAnalysis {

	private static Logger logger = Logger.getLogger(BarcodeAnalysis.class.getName());

	private BarcodeAnalysis() {
		// Prevent instantiation
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
	 * @param outFile Output table
	 * @param suffixFastq1 Also write new fastq file(s) of the reads with all layout elements, and positions before/between them removed.
	 * In other words, keep the part of the read after the last matched element.
	 * Or null if not using.
	 * @param suffixFastq2
	 * @param verbose
	 * @throws IOException
	 */
	private static void findBarcodes(String fastq1, String fastq2, BarcodedReadLayout layout1, BarcodedReadLayout layout2,
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
	
	
	private enum CommandLineOption {
		
		CONFIG_FILE("-cf", "Config file", null) {
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
		
		CommandLineParser p = CommandLineOption.createCommandLineParser();
		p.parse(args);
		if(p.getBooleanArg(CommandLineOption.DEBUG.getFlag())) {
			ReadLayout.logger.setLevel(Level.DEBUG);
			BarcodeSet.logger.setLevel(Level.DEBUG);
			BarcodeSequence.logger.setLevel(Level.DEBUG);
			FragmentBarcode.logger.setLevel(Level.DEBUG);
			BasicBarcodedFragment.logger.setLevel(Level.DEBUG);
			FixedSequence.logger.setLevel(Level.DEBUG);
			AnySequence.logger.setLevel(Level.DEBUG);
			BarcodeAnalysis.logger.setLevel(Level.DEBUG);
			GenericElementMatcher.logger.setLevel(Level.DEBUG);
			BitapMatcher.logger.setLevel(Level.DEBUG);
			//HashMatcher.logger.setLevel(Level.DEBUG);
			//ReadLayoutSequenceHash.logger.setLevel(Level.DEBUG);
		}
		
		BarcodeAnalysisConfigFile configFile = new BarcodeAnalysisConfigFile(p.getStringArg(CommandLineOption.CONFIG_FILE.getFlag()));
		boolean verbose = p.getBooleanArg(CommandLineOption.VERBOSE_OUTPUT.getFlag());
		
		if(configFile.isPaired()) {
			findBarcodes(configFile.getPairedFastq1(), configFile.getPairedFastq2(), 
					configFile.getRead1Layout(), configFile.getRead2Layout(), 
					configFile.getOutputPrefix(), configFile.getOutputSuffixFastq1(), 
					configFile.getOutputSuffixFastq2(), verbose);
		} else {
			findBarcodes(configFile.getSingleFastq(), configFile.getUnpairedReadLayout(), configFile.getOutputPrefix(), 
					verbose, false, configFile.getOutputSuffixFastqUnpaired());
		}
		
		logger.info("");
		logger.info("All done.");
				
	}

}
