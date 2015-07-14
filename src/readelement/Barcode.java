package readelement;

import guttmanlab.core.alignment.SmithWatermanAlignment;
import guttmanlab.core.util.StringParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.sleepycat.persist.model.Persistent;

import nextgen.core.utils.AlignmentUtils;
import nextgen.core.utils.FileUtil;

/**
 * A single barcode
 * @author prussell
 *
 */
@Persistent
public class Barcode extends AbstractReadSequenceElement implements Comparable<Barcode> {
	
	protected String sequence;
	protected String id;
	private int maxNumMismatches;
	public static Logger logger = Logger.getLogger(Barcode.class.getName());
	private boolean repeatable;
	private String stopSignal;
	private int stopSignalMaxMismatches;
	private int length;
	// Smith waterman parameters
	private static float SW_MATCH_SCORE = 5;
	private static float SW_MISMATCH_SCORE = -4;
	private static float SW_GAP_OPEN_PENALTY = 8;
	private static float SW_GAP_EXTEND_PENALTY = 2;

	
	/**
	 * For Berkeley DB only
	 * Do not use this constructor
	 */
	public Barcode() {}
	
	/**
	 * @param seq The barcode sequence
	 */
	public Barcode(String seq) {
		this(seq, -1, false, null, -1);
	}
	
	/**
	 * @param seq The barcode sequence
	 * @param maxMismatches Max allowable number of mismatches when identifying this barcode in reads
	 */
	public Barcode(String seq, int maxMismatches) {
		this(seq, maxMismatches, false, null, -1);
	}
	
	/**
	 * @param seq The barcode sequence
	 * @param maxMismatches Max allowable number of mismatches when identifying this barcode in reads
	 * @param isRepeatable Whether this barcode can appear multiple times in tandem
	 * @param stopSignalForRepeatable String whose presence in read signals the end of the region where this barcode is expected to be found
	 * @param stopSignalMaxMismatch Max mismatches to count a match for stop signal
	 */
	public Barcode(String seq, int maxMismatches, boolean isRepeatable, String stopSignalForRepeatable, int stopSignalMaxMismatch) {
		this(seq, null, maxMismatches, isRepeatable, stopSignalForRepeatable, stopSignalMaxMismatch);
	}

	/**
	 * @param seq The barcode sequence
	 * @param barcodeId Unique ID for this barcode
	 * @param maxMismatches Max allowable number of mismatches when identifying this barcode in reads
	 */
	public Barcode(String seq, String barcodeId, int maxMismatches) {
		this(seq, barcodeId, maxMismatches, false, null, -1);
	}
	
	/**
	 * @param seq The barcode sequence
	 * @param barcodeId Unique ID for this barcode
	 */
	public Barcode(String seq, String barcodeId) {
		this(seq, barcodeId, -1, false, null, -1);
	}
	
	/**
	 * @param seq The barcode sequence
	 * @param barcodeId Unique ID for this barcode
	 * @param maxMismatches Max allowable number of mismatches when identifying this barcode in reads
	 * @param isRepeatable Whether this barcode can appear multiple times in tandem
	 * @param stopSignalForRepeatable String whose presence in read signals the end of the region where this barcode is expected to be found
	 * @param stopSignalMaxMismatch Max mismatches to count a match for stop signal
	 */
	public Barcode(String seq, String barcodeId, int maxMismatches, boolean isRepeatable, String stopSignalForRepeatable, int stopSignalMaxMismatch) {;
		sequence = seq;
		id = barcodeId;
		maxNumMismatches = maxMismatches;
		//matchedStrings = new HashSet<String>();
		repeatable = isRepeatable;
		stopSignal = stopSignalForRepeatable;
		stopSignalMaxMismatches = stopSignalMaxMismatch;
		length = sequence.length();
	}
	
	/**
	 * Create a set of barcodes with IDs from a table file
	 * Line format: barcode_id	barcode_sequence
	 * @param tableFile Table file
	 * @param maxMismatches Max allowable mismatches when matching barcodes
	 * @return Collection of barcodes
	 * @throws IOException
	 */
	public static Collection<Barcode> createBarcodesFromTable(String tableFile, int maxMismatches) throws IOException {
		FileReader r = new FileReader(tableFile);
		BufferedReader b = new BufferedReader(r);
		StringParser s = new StringParser();
		Collection<Barcode> rtrn = new TreeSet<Barcode>();
		while(b.ready()) {
			s.parse(b.readLine());
			if(s.getFieldCount() == 0) {
				continue;
			}
			if(s.getFieldCount() != 2) {
				r.close();
				b.close();
				throw new IllegalArgumentException("Format: barcode_id  barcode_sequence");
			}
			rtrn.add(new Barcode(s.asString(1), s.asString(0), maxMismatches));
		}
		r.close();
		b.close();
		return rtrn;
	}

	/**
	 * Create a set of barcodes without IDs from a list file
	 * @param listFile File with one barcode sequence per line
	 * @param maxMismatches Max allowable mismatches when matching barcodes
	 * @return Collection of barcodes
	 * @throws IOException
	 */
	public static Collection<Barcode> createBarcodesFromList(String listFile, int maxMismatches) throws IOException {
		return createBarcodes(FileUtil.fileLinesAsList(listFile), maxMismatches);
	}
	
	/**
	 * Create barcode objects from a collection of barcode sequences
	 * @param barcodeSeqs Barcode sequences
	 * @param maxMismatches Max allowable mismatches in each barcode when matching to read sequences
	 * @return Collection of barcode objects
	 */
	public static Collection<Barcode> createBarcodes(Collection<String> barcodeSeqs, int maxMismatches) {
		Collection<Barcode> rtrn = new TreeSet<Barcode>();
		for(String b : barcodeSeqs) {
			rtrn.add(new Barcode(b, maxMismatches));
		}
		return rtrn;
	}
	
	@Override
	public int compareTo(Barcode o) {
		int c = sequence.compareTo(o.getSequence());
		if(c != 0 || id == null) {
			return c;
		}
		return id.compareTo(o.getId());
	}

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public String getId() {
		return id;
	}
	
	/**
	 * Get the barcode sequence
	 * @return The barcode sequence
	 */
	public String getSequence() {
		return sequence;
	}

	/**
	 * Get the mismatch tolerance for this barcode
	 * @return Max mismatches
	 */
	public int getMismatchTolerance() {
		return maxNumMismatches;
	}
	
	@Override
	public boolean matchesFullString(String s) {
		if(getLength() != s.length()) {
			return false;
		}
		if(s.equalsIgnoreCase(sequence)) {
			return true;
		}
		return AlignmentUtils.hammingDistanceAtMost(s, sequence, maxNumMismatches, true);
	}

	@Override
	public String elementName() {
		return "barcode";
	}

	@Override
	public boolean matchesSubstringNoGaps(String s, int startOnString) {
		return matchesFullString(s.substring(startOnString, startOnString + getLength()));
	}

	@Override
	public MatchedElement matchedElement(String s) {
		if(matchesSubstringNoGaps(s, 0)) {
			return new MatchedElement(this, length);
		}
		jaligner.Alignment align = SmithWatermanAlignment.align(s, sequence, SW_MATCH_SCORE, SW_MISMATCH_SCORE, SW_GAP_OPEN_PENALTY, SW_GAP_EXTEND_PENALTY);
		if(align.getStart1() != 0) {
			return null; // Must match beginning of string
		}
		int matches = align.getNumberOfMatches();
		int nonMatch = length - matches;
		if(nonMatch > maxNumMismatches) {
			return null; //TODO is this how we want to count indels?
		}
		int lengthOnSeq1 = align.getNumberOfMatches() + align.getGaps2(); //TODO is this right?
		return new MatchedElement(this, lengthOnSeq1);
	}

	@Override
	public boolean isRepeatable() {
		return repeatable;
	}

	@Override
	public ReadSequenceElement getStopSignalForRepeatable() {
		return new FixedSequence("stop_signal", stopSignal, stopSignalMaxMismatches);
	}

	@Override
	public Map<String, ReadSequenceElement> sequenceToElement() {
		Map<String, ReadSequenceElement> rtrn = new HashMap<String, ReadSequenceElement>();
		rtrn.put(sequence, this);
		return rtrn;
	}

	@Override
	public int minMatch() {
		return length - maxNumMismatches;
	}


}
