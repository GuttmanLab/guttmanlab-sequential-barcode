package programs;

import fragmentgroup.FragmentGroup;
import fragmentgroup.BarcodedFragmentGroup;
import guttmanlab.core.util.CommandLineParser;
import guttmanlab.core.util.CountLogger;
import guttmanlab.core.util.StringParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.Session;

import guttmanlab.core.pipeline.Job;
import guttmanlab.core.pipeline.JobUtils;
import guttmanlab.core.pipeline.OGSJob;
import guttmanlab.core.pipeline.Scheduler;
import guttmanlab.core.pipeline.util.BamUtils;
import sequentialbarcode.BarcodeSequence;
import sequentialbarcode.BarcodedFragment;
import sequentialbarcode.BasicBarcodedFragment;
import net.sf.samtools.BAMFileWriter;
import net.sf.samtools.SAMFileReader;
import net.sf.samtools.SAMFormatException;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMRecordIterator;
import nextgen.core.pipeline.util.OGSUtils;

/**
 * Tools to add barcode tag to a bam file and write new bam file
 * @author prussell
 *
 */
public final class BarcodedBamWriter {
	
	private BarcodedBamWriter() {
		// Prevent instantiation
	}
	
	private static Session drmaaSession;

	/**
	 * Sam tag for barcode attribute
	 */
	public static String BARCODES_SAM_TAG = "XB";
	
	/**
	 * Sam tag for fragment group attribute
	 */
	public static String FRAGMENT_GROUP_SAM_TAG = "XF";
	
	/**
	 * Column of barcode tables that contains the barcode
	 */
	private static int BARCODE_COL_NUM = 1;
	
	private static Logger logger = Logger.getLogger(BarcodedBamWriter.class.getName());
	
	/**
	 * Set the barcode attribute of a SAM record
	 * @param record SAM record
	 * @param barcodes Barcodes to set
	 */
	private static void setBarcodes(SAMRecord record, BarcodeSequence barcodes) {
		record.setAttribute(BARCODES_SAM_TAG, barcodes.toSamAttributeString());
	}
	
	/**
	 * Set the fragment group attribute of a SAM record
	 * @param record SAM record
	 * @param fragments Fragment group to set
	 */
	private static void setFragmentGroup(SAMRecord record, FragmentGroup fragments) {
		record.setAttribute(FRAGMENT_GROUP_SAM_TAG, fragments.toSamAttributeString());
	}
	
	/**
	 * Read 2 column table into a map
	 * @param tableFile Table file
	 * @return Map of first column to second column
	 * @throws IOException
	 */
	private static Map<String, String> fromTable(String tableFile) throws IOException{
		logger.info("");
		logger.info("Reading information from table " + tableFile);
		FileReader r = new FileReader(tableFile);
		BufferedReader b = new BufferedReader(r);
		StringParser s = new StringParser();
		Map<String, String> rtrn = new HashMap<String, String>();
		while(b.ready()) {
			s.parse(b.readLine());
			rtrn.put(s.asString(0), s.asString(1));
		}
		r.close();
		b.close();
		logger.info("Done reading table.");
		return rtrn;
	}
	
	/**
	 * Using barcode mapping, add barcode and fragment group attributes to a bam file
	 * @param inputBam Input bam file
	 * @param barcodesByReadId Map of read ID to barcode sequence
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private static void writeBarcodedGroupedBam(String inputBam, Map<String, BarcodeSequence> barcodesByReadId) throws IOException {
		
		String barcodedBam = getBarcodedBamFileName(inputBam);
		File barcodedBamFile = new File(barcodedBam);
		if(!barcodedBamFile.exists()) {
			writeBarcodedBam(inputBam, barcodesByReadId);
		}
		
		String outputBam = getBarcodedGroupedBamFileName(inputBam);
		
		Map<BarcodeSequence, FragmentGroup> fragmentGroupsByBarcode = getFragmentGroupsByBarcode(barcodedBam, getNameMappingFileName(barcodedBam));
		
		logger.info("");
		logger.info("Writing grouped version of " + barcodedBam + " to " + outputBam + "...");
		
		BAMFileWriter w = new BAMFileWriter(new File(outputBam));
		
		// Get header from original bam file
		SAMFileReader r = new SAMFileReader(new File(barcodedBam));
		w.setHeader(r.getFileHeader());
		
		int numDone = 0;
		// Go through the bam file in order
		SAMRecordIterator iter = r.iterator();
		while(iter.hasNext()) {
			if(numDone != 0 && numDone % 10000 == 0) {
				logger.info("Finished " + numDone + " reads.");
			}
			SAMRecord record = iter.next();
			BarcodeSequence barcodes = BarcodeSequence.fromSamRecord(record);
			if(fragmentGroupsByBarcode.containsKey(barcodes)) {
				setFragmentGroup(record, fragmentGroupsByBarcode.get(barcodes));
			}
			w.addAlignment(record);
			numDone++;
		}
		
		r.close();		
		w.close();
		
		logger.info("Done writing file.");
		

	}
	
	private static String BARCODED_BAM_SUFFIX = ".barcode.bam";
	private static String GROUPED_BAM_SUFFIX = ".grouped.bam";
	private static String NAME_MAPPING_SUFFIX = ".shortNameMapping";
	
	/**
	 * Get name of barcoded grouped bam file
	 * @param inputBam Regular or barcoded bam file
	 * @return Name of barcoded grouped bam file
	 */
	private static String getBarcodedGroupedBamFileName(String inputBam) {
		return getBarcodedBamFileName(inputBam).replaceAll(".bam", "") + GROUPED_BAM_SUFFIX;
	}
	
	/**
	 * Get name of barcoded bam file
	 * @param inputBam Regular bam file
	 * @return Name of barcoded bam file
	 */
	private static String getBarcodedBamFileName(String inputBam) {
		return inputBam.replaceAll(".bam", "") + BARCODED_BAM_SUFFIX;
	}
	
	/**
	 * Get name of names mapping file, a table with mapping from full read name to shortened read name used in the grouped bam file
	 * @param inputBam Regular or barcoded bam file
	 * @return Names mapping file name
	 */
	private static String getNameMappingFileName(String inputBam) {
		String s1 = inputBam.replaceAll(BARCODED_BAM_SUFFIX, "");
		return s1.replaceAll(".bam", "") + NAME_MAPPING_SUFFIX;
	}
	
	
	/**
	 * Add barcode attribute to bam file entries using a barcode mapping
	 * @param inputBam Regular bam file
	 * @param barcodeTable Table of read name and barcode sequence
	 * @param overrideOutputName Name for output bam file other than default. Warning: file will not be recognized by other code.
	 * @throws IOException
	 */
	private static void writeBarcodedBam(String inputBam, String barcodeTable, String overrideOutputName) throws IOException {
		writeBarcodedBam(inputBam, readBarcodesFromTable(barcodeTable), overrideOutputName);
	}
	
	/**
	 * Add barcode attribute to bam file entries using a barcode mapping
	 * Use OGS to batch out writing of barcoded bam file then merge bam files at the end
	 * @param inputBam Regular bam file
	 * @param barcodeTable Table of read name and barcode sequence
	 * @param drmaaSession Active DRMAA session
	 * @param readsPerJob Number of reads for each OGS job
	 * @param barcodedBamWriterJar Jar file of BarcodedBamWriter class
	 * @param picardJarDir Directory containing Picard jar files
	 * @throws IOException 
	 * @throws DrmaaException 
	 * @throws InterruptedException 
	 */
	private static void batchWriteBarcodedBam(String inputBam, String barcodeTable, Session drmaaSession, int readsPerJob, String barcodedBamWriterJar, String picardJarDir) throws IOException, DrmaaException, InterruptedException {
		logger.info("");
		logger.info("Adding barcode attribute to bam file " + inputBam + " by batching out to OGS cluster.");
		logger.info("Reading barcodes from " + barcodeTable + ".");
		logger.warn("Not enforcing same reads in bam file and table!");
		
		FileReader r = new FileReader(barcodeTable);
		BufferedReader b = new BufferedReader(r);

		Collection<Job> jobs = new ArrayList<Job>();
		Collection<String> outBams = new ArrayList<String>();
				
		int numSplitBarcodeTables = splitBarcodeTable(barcodeTable, readsPerJob);
		
		for(int jobNum = 0; jobNum < numSplitBarcodeTables; jobNum++) {
			String outBam = getBarcodedBamFileName(inputBam) + "." + jobNum;
			outBams.add(outBam);
			String cmmd = "java -jar -Xmx27g -Xms25g -Xmn20g " + barcodedBamWriterJar + " -b false -ib " + inputBam + " -bc " + BARCODE_COL_NUM + " -bt " + barcodeTable + "." + jobNum + " -on " + outBam;
			logger.info("Submitting OGS job: " + cmmd);
			OGSJob job = new OGSJob(drmaaSession, cmmd);
			job.submit();
			jobs.add(job);
		}
		
		r.close();
		b.close();

		// Wait for jobs
		logger.info("");
		logger.info("Waiting for " + jobs.size() + " jobs.");
		JobUtils.waitForAll(jobs);
		
		// Merge bam files
		BamUtils.mergeBams(outBams, getBarcodedBamFileName(inputBam), Scheduler.OGS, drmaaSession, picardJarDir, false);
		
		// Delete temp files
		for(String bam : outBams) {
			File file = new File(bam);
			file.delete();
		}
				
	}
	
	/**
	 * Add barcode attribute to bam file entries using a barcode mapping
	 * @param inputBam Regular bam file
	 * @param barcodesByReadId Map of read name to barcode sequence
	 * @throws IOException
	 */
	private static void writeBarcodedBam(String inputBam, Map<String, BarcodeSequence> barcodesByReadId) throws IOException {
		writeBarcodedBam(inputBam, barcodesByReadId, null);
	}
	
	/**
	 * Add barcode attribute to bam file entries using a barcode mapping
	 * @param inputBam Regular bam file
	 * @param barcodesByReadId Map of read name to barcode sequence
	 * @param overrideOutputName Name for output bam file other than default. Warning: file will not be recognized by other code. Pass null to use default.
	 * @throws IOException
	 */
	private static void writeBarcodedBam(String inputBam, Map<String, BarcodeSequence> barcodesByReadId, String overrideOutputName) throws IOException {
		
		String outputBam = overrideOutputName == null ? getBarcodedBamFileName(inputBam) : overrideOutputName;
		//String outputNames = getNameMappingFileName(outputBam);
		
		logger.info("");
		logger.info("Writing barcoded version of " + inputBam + " to " + outputBam + "...");
		//logger.info("Writing mapping of read names to shortened IDs to " + outputNames + "...");
		
		BAMFileWriter w = new BAMFileWriter(new File(outputBam));
		//FileWriter nw = new FileWriter(outputNames);
		
		// Get header from original bam file
		SAMFileReader r = new SAMFileReader(new File(inputBam));
		w.setHeader(r.getFileHeader());
		
		int numDone = 0;
		int skipped = 0;
		int unmapped = 0;
		// Go through the bam file in order
		SAMRecordIterator iter = r.iterator();
		//int recordNum = 0;
		while(iter.hasNext()) {
			numDone++;
			if(numDone != 0 && numDone % 100000 == 0) {
				logger.info("Finished " + numDone + " reads. Skipped " + unmapped + " unmapped reads and " + skipped + " reads not in map.");
			}
			try {
				SAMRecord record = iter.next();
				String oldName = record.getReadName();
				//String newName = "r" + recordNum;
				//nw.write(oldName + "\t" + newName + "\n");
				//recordNum++;
				
				if(record.getReadUnmappedFlag()) {
					unmapped++;
					continue;
				}
				if(!barcodesByReadId.containsKey(oldName)) {
					logger.debug("READ_NOT_FOUND\t" + oldName);
					skipped++;
					continue;
				}
				BarcodeSequence barcodes = barcodesByReadId.get(oldName);
				setBarcodes(record, barcodes);
				w.addAlignment(record);
			} catch(SAMFormatException e) {
				logger.info("Caught SAM format exception. Skipping read.");
				continue;
			}
		}
		
		r.close();		
		w.close();
		//nw.close();
		
		logger.info("Done writing file.");
				
	}
	
	/**
	 * Read a specified number of barcodes from an existing buffered reader
	 * @param tableReader Buffered reader for barcode table file with line format: read_ID   barcode_sequence_as_SAM_attribute
	 * @param numToGet Number of reads to get
	 * @return The requested number of reads or less if runs out, or null if reader is not ready
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private static Map<String, BarcodeSequence> readNextBarcodesFromTable(BufferedReader tableReader, int numToGet) throws IOException {
		
		if(!tableReader.ready()) {
			return null;
		}
		
		logger.info("");
		logger.info("Reading next " + numToGet + " fragment barcodes from file...");
		
		int numDone = 0;
		Map<String, BarcodeSequence> rtrn = new HashMap<String, BarcodeSequence>();
		StringParser s = new StringParser();
		while(tableReader.ready() && numDone < numToGet) {
			numDone++;
			if(numDone % 100000 == 0) {
				logger.info("Finished " + numDone);
			}
			s.parse(tableReader.readLine());
			String name = s.asString(0);
			rtrn.put(name, BarcodeSequence.fromSamAttributeString(s.asString(1)));
		}
		return rtrn;
	}
	
	/**
	 * Read a specified number of barcodes from an existing buffered reader
	 * @param tableReader Buffered reader for barcode table file with line format: read_ID   barcode_sequence_as_SAM_attribute
	 * @param numToGet Number of reads to get
	 * @param outTable Output table
	 * @return True if wrote some lines, false if there were no more lines to write
	 * @throws IOException
	 */
	private static boolean writeNextBarcodesFromTable(BufferedReader tableReader, int numToGet, String outTable) throws IOException {
		
		if(!tableReader.ready()) {
			return false;
		}
		
		logger.info("");
		logger.info("Writing next " + numToGet + " fragment barcodes to file " + outTable + "...");
		
		FileWriter w = new FileWriter(outTable);
		
		int numDone = 0;
		while(tableReader.ready() && numDone < numToGet) {
			numDone++;
			w.write(tableReader.readLine() + "\n");
		}
		
		w.close();
		
		logger.info("Done writing to " + outTable + ".");
		
		return true;
		
	}
	
	private static class SplitBarcodeTableFileNameFilter implements FilenameFilter {
		
		private String barcodeTable;
		
		public SplitBarcodeTableFileNameFilter(String barcodeTableFileName) {
			barcodeTable = barcodeTableFileName;
		}
		
		@Override
		public boolean accept(File dir, String name) {
			return name.contains(barcodeTable + ".");
		}
		
	}
	
	private static int splitBarcodeTable(String barcodeTable, int readsPerJob) throws IOException {
		
		BufferedReader reader = new BufferedReader(new FileReader(barcodeTable));
		
		SplitBarcodeTableFileNameFilter filter = new SplitBarcodeTableFileNameFilter(barcodeTable);
		File parentDir = new File(barcodeTable).getParentFile();
		String[] splitFiles = parentDir != null ? parentDir.list(filter) : new File(".").list(filter);
		if(splitFiles.length > 0) {
			logger.warn(splitFiles.length + " SPLIT BARCODE FILES ALREADY EXIST. NOT REGENERATING.");
			reader.close();
			return splitFiles.length;
		}
		
		int jobNum = 0;
		while(writeNextBarcodesFromTable(reader, readsPerJob, barcodeTable + "." + jobNum)) {
			jobNum++;
		}
		reader.close();
		
		return jobNum;
		
	}
	
	/**
	 * Get mapping of read ID to barcode sequence from a table
	 * @param tableFile Table file with line format: read_ID   barcode_sequence_as_SAM_attribute
	 * @return Mapping of read ID to barcode in the read
	 * @throws IOException
	 */
	private static Map<String, BarcodeSequence> readBarcodesFromTable(String tableFile) throws IOException {
		
		logger.info("");
		logger.info("Reading fragment barcodes from file " + tableFile + "...");
		
		int numDone = 0;
		Map<String, BarcodeSequence> rtrn = new HashMap<String, BarcodeSequence>();
		FileReader r = new FileReader(tableFile);
		BufferedReader b = new BufferedReader(r);
		StringParser s = new StringParser();
		while(b.ready()) {
			numDone++;
			if(numDone % 100000 == 0) {
				logger.info("Finished " + numDone);
			}
			s.parse(b.readLine());
			String name = s.asString(0);
			rtrn.put(name, BarcodeSequence.fromSamAttributeString(s.asString(BARCODE_COL_NUM)));
			logger.debug("GOT_READ\t" + name);
		}
		r.close();
		b.close();
		logger.info("Done reading from table.");
		return rtrn;
	}
	
	/**
	 * Get name of fragment group table based on barcode table file name
	 * @param barcodeTable Barcode table
	 * @return Name of fragment group table
	 */
	private static String getFragmentGroupTableName(String barcodeTable) {
		return barcodeTable.replaceAll(".bam", "") + ".fragment_groups";
	}
	
	/**
	 * Read barcoded bam file and determine fragment groups
	 * @param barcodedBam Barcoded bam file
	 * @param shortenedReadNameTable Table with shortened read names. Line format: read_ID   shortened_read_name
	 * @return Map of barcode sequence to the group of fragments sharing those barcodes
	 * @throws IOException
	 */
	private static Map<BarcodeSequence, FragmentGroup> getFragmentGroupsByBarcode(String barcodedBam, String shortenedReadNameTable) throws IOException {
		logger.info("");
		logger.info("Reading fragments by barcode from " + barcodedBam);
		
		String tableFile = getFragmentGroupTableName(barcodedBam);
		if(new File(tableFile).exists()) {
			return readFragmentGroupsFromTable(tableFile);
		}
		
		Map<String,String> shortNames = fromTable(shortenedReadNameTable);
		
		Map<BarcodeSequence, FragmentGroup> rtrn = new HashMap<BarcodeSequence, FragmentGroup>();
		SAMFileReader r = new SAMFileReader(new File(barcodedBam));
		SAMRecordIterator iter = r.iterator();
		int numDone = 0;
		while(iter.hasNext()) {
			numDone++;
			if(numDone % 100000 == 0) {
				logger.info("Read " + numDone + " reads. There are " + rtrn.size() + " different barcode signatures.");
			}
			SAMRecord record = iter.next();
			@SuppressWarnings("unused")
			String dummy = record.getSAMString(); // This somehow prevents a picard bug from surfacing. Do not remove.
			record.setReadName(shortNames.get(record.getReadName()));
			BarcodedFragment fragment = new BasicBarcodedFragment(record);
			BarcodeSequence b = fragment.getBarcodes();
			if(!rtrn.containsKey(b)) {
				rtrn.put(b, new BarcodedFragmentGroup(b));
			}
			rtrn.get(b).addFragment(fragment);
			record = null;
		}
		logger.info("Got fragments for " + rtrn.size() + " different barcode signatures.");
		r.close();
		
		logger.info("");
		logger.info("Writing fragments by barcode in " + barcodedBam + " to file " + tableFile);
		FileWriter w = new FileWriter(tableFile);
		CountLogger cl = new CountLogger(rtrn.size(), 100);
		for(BarcodeSequence barcodes : rtrn.keySet()) {
			cl.advance();
			w.write(barcodes.toSamAttributeString() + "\t" + rtrn.get(barcodes).toSamAttributeString() + "\n");
		}
		w.close();
		logger.info("Done writing file.");
		
		return rtrn;
	}
	
	/**
	 * Read fragment groups from table
	 * @param fragmentGroupsTable Table with line format: barcode_sequence_sam_attribute_string  fragment_group_sam_attribute_string
	 * @return Map of barcode sequence to the group of fragments with the barcodes
	 * @throws IOException
	 */
	private static Map<BarcodeSequence, FragmentGroup> readFragmentGroupsFromTable(String fragmentGroupsTable) throws IOException {
		logger.info("");
		logger.info("Reading fragment groups from table " + fragmentGroupsTable);
		Map<BarcodeSequence, FragmentGroup> rtrn = new HashMap<BarcodeSequence, FragmentGroup>();
		FileReader r = new FileReader(fragmentGroupsTable);
		BufferedReader b = new BufferedReader(r);
		StringParser s = new StringParser();
		int numDone = 0;
		while(b.ready()) {
			numDone++;
			if(numDone % 100000 == 0) {
				logger.info("Finished " + numDone + " barcodes.");
			}
			s.parse(b.readLine());
			if(s.getFieldCount() != 2) {
				b.close();
				throw new IllegalArgumentException("Line format: barcode_signature   fragment_group");
			}
			BarcodeSequence barcodes = BarcodeSequence.fromSamAttributeString(s.asString(0));
			FragmentGroup fragments = BarcodedFragmentGroup.createFromSamAttributeStrings(s.asString(0), s.asString(1));
			rtrn.put(barcodes, fragments);
		}
		r.close();
		b.close();
		logger.info("Got fragment groups for " + rtrn.size() + " barcodes.");
		return rtrn;
	}

	public static void main(String[] args) throws IOException, DrmaaException, InterruptedException {
		

		CommandLineParser p = new CommandLineParser();
		p.addStringArg("-ib", "Input bam file", true);
		p.addStringArg("-bt", "Table of barcodes by read name", true);
		p.addStringArg("-on", "Output bam file name to override default. Warning: other code will not recognize this file.", false);
		p.addBooleanArg("-d", "Debug logging on", false, false);
		p.addIntArg("-rj", "When batching out writing of barcoded bam file, number of reads per job", false, 10000000);
		p.addStringArg("-bbj", "Barcoded bam writer jar file (needed for batching)", false, null);
		p.addStringArg("-pj", "Picard jar director (needed for batching)", false, null);
		p.addBooleanArg("-b", "Batch out to cluster", false, false);
		p.addIntArg("-bc", "Column in barcode tables that contains the barcode", true);
		p.parse(args);
		
		BARCODE_COL_NUM = p.getIntArg("-bc");
		
		if(p.getBooleanArg("-d")) {
			logger.setLevel(Level.DEBUG);
		}
		
		String inputBam = p.getStringArg("-ib");
		String barcodeTable = p.getStringArg("-bt");
		String overrideName = p.getStringArg("-on");
		int readsPerJob = p.getIntArg("-rj");
		String barcodedBamWriterJar = p.getStringArg("-bbj");
		String picardJarDir = p.getStringArg("-pj");
		
		if(p.getBooleanArg("-b")) {
			drmaaSession = OGSUtils.getDrmaaSession();
			if(barcodeTable == null) {
				throw new IllegalArgumentException("Must provide table of barcodes by read name with -bt option");
			}
			if(barcodedBamWriterJar == null) {
				throw new IllegalArgumentException("Must provide BarcodedBamWriter.jar file with -bbj option");
			}
			if(picardJarDir == null) {
				throw new IllegalArgumentException("Must provide Picard jar directory with -pj option");
			}
			batchWriteBarcodedBam(inputBam, barcodeTable, drmaaSession, readsPerJob, barcodedBamWriterJar, picardJarDir);
		} else {
			writeBarcodedBam(inputBam, barcodeTable, overrideName);
		}
		
		logger.info("");
		logger.info("All done.");
		
	}
	
	
	
	
}
