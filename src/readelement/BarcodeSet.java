package readelement;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import matcher.MatchedElement;

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
	protected Collection<Barcode> barcodes;
	private int maxLevDist;
	
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
		maxLevDist = barcodeSet.iterator().next().maxLevenshteinDist();
		for(Barcode b : barcodeSet) {
			if(b.getLength() != len) {
				throw new IllegalArgumentException("All barcode sequences must have the same length");
			}
			if(b.maxLevenshteinDist() != maxLevDist) {
				throw new IllegalArgumentException("All barcodes in set must have same max Levenshtein distance");
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
	public String elementName() {
		return "barcode_set";
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public MatchedElement matchedElement(String s) {
		throw new UnsupportedOperationException("NA");
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

	@Override
	public Map<String, ReadSequenceElement> sequenceToElement() {
		Map<String, ReadSequenceElement> rtrn = new HashMap<String, ReadSequenceElement>();
		for(Barcode barcode : barcodes) {
			rtrn.putAll(barcode.sequenceToElement());
		}
		return rtrn;
	}

	@Override
	public int minMatch() {
		throw new UnsupportedOperationException("NA");
	}

	@Override
	public int maxLevenshteinDist() {
		return maxLevDist;
	}



}
