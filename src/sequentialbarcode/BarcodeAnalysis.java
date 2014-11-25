package sequentialbarcode;

import guttmanlab.core.util.CommandLineParser;
import guttmanlab.core.util.StringParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.Session;

import sequentialbarcode.readlayout.AnySequence;
import sequentialbarcode.readlayout.Barcode;
import sequentialbarcode.readlayout.BarcodeSet;
import sequentialbarcode.readlayout.BarcodedReadLayout;
import sequentialbarcode.readlayout.FixedSequence;
import sequentialbarcode.readlayout.ReadLayout;
import sequentialbarcode.readlayout.ReadLayoutFactory;
import sequentialbarcode.readlayout.LigationDesign;
import guttmanlab.core.pipeline.Job;
import guttmanlab.core.pipeline.JobUtils;
import guttmanlab.core.pipeline.OGSJob;
import guttmanlab.core.pipeline.util.FastqUtils;
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
		
		String outRD3 = p.getStringArg(outRD3Option);
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
		boolean enforceOddEven = p.getBooleanArg(enforceOddEvenOption);
		boolean verbose = p.getBooleanArg(verboseOption);
		String jar = p.getStringArg(jarOption);
		int numFastq = p.getIntArg(numFastqOption);
		
		Collection<String> splitFastqs = FastqUtils.divideFastqFile(fastq, numFastq);
		Map<String, String> splitTables = new TreeMap<String, String>();
		int i = 0;
		for(String fq : splitFastqs) {
			splitTables.put(fq, outRD3 + "." + i);
			i++;
		}
		Collection<Job> jobs = new ArrayList<Job>();
		for(String fq : splitTables.keySet()) {
			String cmmd = "java -jar -Xmx25g -Xms15g -Xmn10g " + jar;
			cmmd += " " + outRD3Option + " " + splitTables.get(fq);
			cmmd += " " + fastqOption + " " + fq;
			cmmd += " " + readLengthOption + " " + readLength;
			cmmd += " " + oddBarcodeListOption + " " + oddBarcodeList;
			cmmd += " " + evenBarcodeListOption + " " + evenBarcodeList;
			cmmd += " " + rpmOption + " " + rpm;
			cmmd += " " + dpmOption + " " + dpm;
			cmmd += " " + maxMismatchBarcodeOption + " " + maxMismatchBarcode;
			cmmd += " " + maxMismatchRpmOption + " " + maxMismatchRpm;
			cmmd += " " + maxMismatchDpmOption + " " + maxMismatchDpm;
			cmmd += " " + enforceOddEvenOption + " " + enforceOddEven;
			cmmd += " " + verboseOption + " " + verbose;
			String jobName = "OGS_job_" + fq;
			OGSJob job = new OGSJob(drmaaSession, cmmd, true, jobName, email);
			job.submit();
			jobs.add(job);
		}
		JobUtils.waitForAll(jobs);
		FileWriter w = new FileWriter(outRD3);
		for(String fq : splitTables.keySet()) {
			String table = splitTables.get(fq);
			FileReader r = new FileReader(table);
			BufferedReader b = new BufferedReader(r);
			while(b.ready()) {
				w.write(b.readLine() + "\n");
			}
			r.close();
			b.close();
			File f = new File(table);
			f.delete();
		}
		w.close();
		for(String fq : splitFastqs) {
			File f = new File(fq);
			f.delete();
		}
	}
	
	
	/**
	 * For RNA-DNA 3D barcoding method
	 * Identify barcodes in reads and write to a table
	 * @param fastq Fastq file
	 * @param layout Barcoded read layout
	 * @param maxMismatchBarcode Max number of mismatches when matching barcodes to reads
	 * @param outFile Output table
	 * @param verbose Verbose table output
	 * @throws IOException
	 */
	private static void findBarcodes(String fastq, BarcodedReadLayout layout, int maxMismatchBarcode, String outFile, boolean verbose) throws IOException {
		logger.info("");
		logger.info("Identifying read2 barcodes for RNA-DNA-3D and writing to table " + outFile + "...");
		FileWriter w = new FileWriter(outFile);
		FastqParser iter = new FastqParser();
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
			if(layout.getMatchedElements(seq) != null) {
				BarcodedFragment f = new BarcodedFragmentImpl(name, null, seq, null, layout, maxMismatchBarcode);
				BarcodeSequence barcodes = f.getBarcodes();
				if(verbose) line += barcodes.getNumBarcodes() + "\t";
				line += barcodes.toString() + "\t";
				if(verbose) line += seq + "\t";
				w.write(line + "\n");
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
		w.close();
	}
	
	
	/**
	 * For RNA-DNA 3D barcoding method
	 * Count instances of each barcode and print totals
	 * @param fastq Fastq file
	 * @param layout Barcoded read layout
	 * @param maxMismatchBarcode Max number of mismatches when matching barcodes to reads
	 * @throws IOException
	 */
	private static void countBarcodes(String fastq, BarcodedReadLayout layout, int maxMismatchBarcode) throws IOException {
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
			if(layout.getMatchedElements(seq) != null) {
				BarcodedFragment f = new BarcodedFragmentImpl(name, null, seq, null, layout, maxMismatchBarcode);
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
		
		String outRD3 = commandLineParser.getStringArg(outRD3Option);
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
		
		if((outRD3 != null || countBarcodes) && fastq == null) {
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
		case SINGLE_DESIGN_BARCODE_IN_READ2:
			throw new UnsupportedOperationException("Not implemented for " + design.toString());
		default:
			throw new UnsupportedOperationException("Not implemented for " + design.toString());		
		}
	}
	
	private static String debugOption = "-d";
	private static String outRD3Option = "-ob3d";
	private static String emailOption = "-e";
	private static String fastqOption = "-fq";
	private static String readLengthOption = "-rl";
	private static String  oddBarcodeListOption = "-ob";
	private static String  evenBarcodeListOption = "-eb";
	private static String  rpmOption = "-rpm";
	private static String  dpmOption = "-dpm";
	private static String  maxMismatchBarcodeOption = "-mmb";
	private static String  maxMismatchRpmOption = "-mmr";
	private static String  maxMismatchDpmOption = "-mmd";
	private static String  enforceOddEvenOption = "-oe";
	private static String  countBarcodesOption = "-cb";
	private static String  verboseOption = "-v";
	private static String  batchOption = "-bt";
	private static String  jarOption = "-j";
	private static String  numFastqOption = "-nf";
	private static String  designNameOption = "-l";
	private static String  totalNumBarcodesOption = "-nb";

	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws DrmaaException 
	 */
	public static void main(String[] args) throws IOException, DrmaaException, InterruptedException {
		
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
		p.addStringArg(outRD3Option, "Output file for barcode identification", false, null);
		p.addStringArg(fastqOption, "Fastq file", false, null);
		p.addIntArg(readLengthOption, "Read length", false, -1);
		p.addStringArg(oddBarcodeListOption, "Odd barcode table file (format: barcode_id	barcode_seq)", false, null);
		p.addStringArg(evenBarcodeListOption, "Even barcode table file (format: barcode_id	barcode_seq)", false, null);
		p.addStringArg(dpmOption, "DPM sequence", false, null);
		p.addStringArg(rpmOption, "RPM sequence", false, null);
		p.addIntArg(maxMismatchBarcodeOption, "Max mismatches in barcode", false, -1);
		p.addIntArg(maxMismatchRpmOption, "Max mismatches in RPM", false, -1);
		p.addIntArg(maxMismatchDpmOption, "Max mismatches in DPM", false, -1);
		p.addBooleanArg(enforceOddEvenOption, "Enforce odd/even alternation for barcodes", false, false);
		p.addBooleanArg(countBarcodesOption, "Count barcodes", false, false);
		p.addBooleanArg(verboseOption, "Verbose output table for barcode identification", false, false);
		p.addBooleanArg(batchOption, "Batch out writing of barcode table", false, false);
		p.addStringArg(jarOption, "This jar file for batched jobs", false, null);
		p.addIntArg(numFastqOption, "Number of fastq files to divide into if batching", false, 20);
		p.addStringArg(emailOption, "Email address for OGS jobs", false, null);
		p.addStringArg(designNameOption, "Ligation design. Options: " + LigationDesign.getNamesAsCommaSeparatedList(), true);
		p.addIntArg(totalNumBarcodesOption, "Total number of barcode ligations", false, -1);
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
		}
		
		String outRD3 = p.getStringArg(outRD3Option);
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
		
		validateCommandLine(p);
		
		// Make table of reads IDs and barcodes
		if(outRD3 != null) {
			if(batch) {
				divideFastqAndFindBarcodes(p);
			} else {
				switch(design) {
				case PAIRED_DESIGN_BARCODE_IN_READ2:
					BarcodedReadLayout layout = ReadLayoutFactory.getRead2LayoutRnaDna3DPairedDesign(evenBarcodeList, oddBarcodeList, totalNumBarcodes, rpm, readLength, maxMismatchBarcode, maxMismatchRpm, enforceOddEven);
					findBarcodes(fastq, layout, maxMismatchBarcode, outRD3, verbose);
					break;
				case SINGLE_DESIGN_BARCODE_IN_READ2:
					throw new IllegalArgumentException("Not implemented");
				default:
					throw new IllegalArgumentException("Not implemented");
				}
			}
		}

		// Make histogram of barcodes
		if(countBarcodes) {
			switch(design) {
			case PAIRED_DESIGN_BARCODE_IN_READ2:
				BarcodedReadLayout layout = ReadLayoutFactory.getRead2LayoutRnaDna3DPairedDesign(evenBarcodeList, oddBarcodeList, totalNumBarcodes, rpm, readLength, maxMismatchBarcode, maxMismatchRpm, enforceOddEven);
				countBarcodes(fastq, layout, maxMismatchBarcode);
				break;
			case SINGLE_DESIGN_BARCODE_IN_READ2:
				throw new IllegalArgumentException("Not implemented");
			default:
				throw new IllegalArgumentException("Not implemented");			
			}
		}
		
		
		logger.info("");
		logger.info("All done.");
				
	}

}
