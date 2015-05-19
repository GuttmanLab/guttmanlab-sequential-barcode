package readelement;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

/**
 * A set of barcodes that are used together in the lab
 * @author prussell
 *
 */
public class BarcodeSet extends AbstractReadSequenceElement {
	
	private Collection<Barcode> barcodes;
	public static Logger logger = Logger.getLogger(BarcodeSet.class.getName());
	private Map<String, Barcode> seqToMatchedElement;
	private Collection<String> noMatch;
	private int length;
	private String id;
	private boolean repeatable;
	private String stopSignalSeq;
	private FixedSequenceCollection stopSignalSeqCollection;
	private int stopSignalMaxMismatches;
	
	/**
	 * @param setId Barcode set ID
	 * @param barcodeSet The barcodes
	 */
	public BarcodeSet(String setId, Collection<Barcode> barcodeSet) {
		this(setId, barcodeSet, false);
	}
			
	/**
	 * @param setId Barcode set ID
	 * @param barcodeSet The barcodes
	 * @param isRepeatable Whether to look for multiple matches in sequence
	 * @param stopSignalForRepeatable String whose presence in a read signals the end of the region that is expected to contain these barcodes
	 */
	public BarcodeSet(String setId, Collection<Barcode> barcodeSet, boolean isRepeatable) {
		id = setId;
		repeatable = isRepeatable;
		seqToMatchedElement = new TreeMap<String, Barcode>();
		noMatch = new TreeSet<String>();
		int len = barcodeSet.iterator().next().getLength();
		barcodes = new TreeSet<Barcode>();
		for(Barcode b : barcodeSet) {
			if(b.getLength() != len) {
				throw new IllegalArgumentException("All barcode sequences must have the same length");
			}
			barcodes.add(b);
		}
		length = len;
	}
	
	/**
	 * @param setId Barcode set ID
	 * @param barcodeSet The barcodes
	 * @param isRepeatable Whether to look for multiple matches in sequence
	 * @param stopSignal String whose presence in a read signals the end of the region that is expected to contain these barcodes
	 * @param stopSignalMaxMismatch Max mismatches to count a match for stop signal
	 */
	public BarcodeSet(String setId, Collection<Barcode> barcodeSet, boolean isRepeatable, String stopSignal, int stopSignalMaxMismatch) {
		this(setId, barcodeSet, isRepeatable);
		setStopSignalAsString(stopSignal);
		stopSignalMaxMismatches = stopSignalMaxMismatch;
	}
	
	/**
	 * @param setId Barcode set ID
	 * @param barcodeSet The barcodes
	 * @param isRepeatable Whether to look for multiple matches in sequence
	 * @param stopSignal Collection of strings whose presence in a read signals the end of the region that is expected to contain these barcodes
	 * @param stopSignalMaxMismatch Max mismatches to count a match for stop signal
	 */
	public BarcodeSet(String setId, Collection<Barcode> barcodeSet, boolean isRepeatable, FixedSequenceCollection stopSignal) {
		setStopSignalAsFixedSequenceCollection(stopSignal);
	}
	
	private void setStopSignalAsString(String stopSignal) {
		stopSignalSeq = stopSignal;
	}
	
	private void setStopSignalAsFixedSequenceCollection(FixedSequenceCollection seqs) {
		stopSignalSeqCollection = seqs;
	}
	
	/**
	 * @return The barcodes
	 */
	public Collection<Barcode> getBarcodes() {
		return barcodes;
	}
	
	@Override
	public int getLength() {
		return length;
	}

	@Override
	public boolean matchesFullString(String s) {
		if(s.length() != length) {
			return false;
		}
		if(seqToMatchedElement.containsKey(s)) {
			return true;
		}
		if(noMatch.contains(s)) {
			return false;
		}
		for(Barcode barcode : barcodes) {
			if(barcode.matchesFullString(s)) {
				seqToMatchedElement.put(s, barcode);
				return true;
			}
		}
		noMatch.add(s);
		return false;
	}


	@Override
	public String elementName() {
		return "barcode_set";
	}

	@Override
	public String matchedElementSequence(String s) {
		return matchedElement(s).matchedElementSequence(s);
	}
	
	@Override
	public boolean matchesSubstringOf(String s, int startOnString) {
		return matchesFullString(s.substring(startOnString, startOnString + getLength()));
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public ReadSequenceElement matchedElement(String s) {
		if(seqToMatchedElement.containsKey(s)) {
			return seqToMatchedElement.get(s);
		}
		if(!matchesFullString(s)) {
			return null;
		}
		return seqToMatchedElement.get(s);
	}

	@Override
	public boolean isRepeatable() {
		return repeatable;
	}

	@Override
	public ReadSequenceElement getStopSignalForRepeatable() {
		if(stopSignalSeq != null) {
			return new FixedSequence("stop_signal", stopSignalSeq, stopSignalMaxMismatches);
		}
		if(stopSignalSeqCollection != null) {
			return stopSignalSeqCollection;
		}
		throw new IllegalStateException("No stop signal specified");
	}


}
