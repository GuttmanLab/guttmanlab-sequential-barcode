package readelement;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * A set of barcodes that are used together in the lab
 * @author prussell
 *
 */
public class BarcodeSet extends AbstractReadSequenceElement {
	
	protected Map<String, Collection<Barcode>> barcodesByPrefix; // Map of barcode prefix to barcode
	public static Logger logger = Logger.getLogger(BarcodeSet.class.getName());
	protected int length;
	private String id;
	private boolean repeatable;
	private String stopSignalSeq;
	private FixedSequenceCollection stopSignalSeqCollection;
	private int stopSignalMaxMismatches;
	protected static int barcodePrefixLen = 2; // Length of barcode prefix to store for indexing
	private Collection<Barcode> barcodes;
	
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
		int len = barcodeSet.iterator().next().getLength();
		barcodesByPrefix = new HashMap<String, Collection<Barcode>>();
		for(Barcode b : barcodeSet) {
			if(b.getLength() != len) {
				throw new IllegalArgumentException("All barcode sequences must have the same length");
			}
			String prefix = b.getSequence().substring(0, barcodePrefixLen);
			if(!barcodesByPrefix.containsKey(prefix)) {
				barcodesByPrefix.put(prefix, new HashSet<Barcode>());
			}
			barcodesByPrefix.get(prefix).add(b);
		}
		length = len;
		barcodes = new HashSet<Barcode>();
		for(Collection<Barcode> bs : barcodesByPrefix.values()) {
			barcodes.addAll(bs);
		}
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
		this(setId, barcodeSet, isRepeatable);
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
		if(matchedElement(s) == null) return false;
		return true;
	}


	@Override
	public String elementName() {
		return "barcode_set";
	}

	@Override
	public boolean matchesSubstringNoGaps(String s, int startOnString) {
		return matchesFullString(s.substring(startOnString, startOnString + getLength()));
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public MatchedElement matchedElement(String s) {
		String prefix = s.substring(0, barcodePrefixLen);
		// Ungapped match for barcodes starting with prefix
		try {
			for(Barcode barcode : barcodesByPrefix.get(prefix)) {
				if(barcode.matchesSubstringNoGaps(s, 0)) {
					return new MatchedElement(this, length);
				}
			}
		} catch (NullPointerException e) {}
		// Ungapped match for barcodes not starting with prefix
		for(Barcode barcode : getBarcodes()) {
			try {
				if(barcodesByPrefix.get(prefix).contains(barcode)) continue;
			} catch(NullPointerException e) {}
			if(barcode.matchesSubstringNoGaps(s, 0)) {
				return new MatchedElement(this, length);
			}
		}
		// Gapped match for barcodes starting with prefix
		try {
			for(Barcode barcode : barcodesByPrefix.get(prefix)) {
				MatchedElement matchedElt = barcode.matchedElement(s);
				if(matchedElt != null) {
					return new MatchedElement(this, length);
				}
			}
		} catch (NullPointerException e) {}
		// Gapped match for barcodes not starting with prefix
		for(Barcode barcode : getBarcodes()) {
			try {
				if(barcodesByPrefix.get(prefix).contains(barcode)) continue;
			} catch(NullPointerException e) {}
			MatchedElement matchedElt = barcode.matchedElement(s);
			if(matchedElt != null) {
				return new MatchedElement(this, length);
			}
		}
		return null;
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

	@Override
	public String getSequence() {
		return null;
	}


}
