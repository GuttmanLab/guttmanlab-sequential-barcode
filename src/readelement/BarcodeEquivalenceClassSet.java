package readelement;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A collection of barcode equivalence classes
 * @author prussell
 *
 */
public class BarcodeEquivalenceClassSet extends AbstractReadSequenceElement {

	private Collection<BarcodeEquivalenceClass> equivClasses;
	private int length;
	private String id;
	private boolean repeatable;
	private String stopSignalSeq;
	private FixedSequenceCollection stopSignalSeqCollection;
	private int stopSignalMaxMismatches;
	private int maxLevDist;
	
	
	
	/**
	 * @param setId Barcode set ID
	 * @param equivalenceClasses The barcode equivalence classes
	 * @param isRepeatable Whether to look for multiple matches in sequence
	 * @param stopSignal Collection of strings whose presence in a read signals the end of the region that is expected to contain these barcodes
	 */
	public BarcodeEquivalenceClassSet(String setId, Collection<BarcodeEquivalenceClass> equivalenceClasses, boolean isRepeatable, FixedSequenceCollection stopSignal) {
		id = setId;
		equivClasses = equivalenceClasses;
		repeatable = isRepeatable;
		stopSignalSeqCollection = stopSignal;
		int len = equivClasses.iterator().next().getLength();
		maxLevDist = equivClasses.iterator().next().maxLevenshteinDist();
		for(BarcodeEquivalenceClass b : equivClasses) {
			if(b.getLength() != len) {
				throw new IllegalArgumentException("All barcode equivalence classes must have the same length");
			}
			if(b.maxLevenshteinDist() != maxLevDist) {
				throw new IllegalArgumentException("All barcode equivalence classes must have the same max Levenshtein distance");
			}
		}
		length = len;
	}
	
	@Override
	public int getLength() {
		return length;
	}

	@Override
	public boolean matchesFullString(String s) {
		for(BarcodeEquivalenceClass ec : equivClasses) {
			if(ec.matchesFullString(s)) return true;
		}
		return false;
	}

	@Override
	public boolean matchesSubstringNoGaps(String s, int startOnString) {
		for(BarcodeEquivalenceClass ec : equivClasses) {
			if(ec.matchesSubstringNoGaps(s, startOnString)) return true;
		}
		return false;
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
	public MatchedElement matchedElement(String s) {
		for(BarcodeEquivalenceClass ec : equivClasses) {
			MatchedElement matchedElt = ec.matchedElement(s);
			if(matchedElt != null) {
				return matchedElt;
			}
		}
		return null;
	}

	@Override
	public String elementName() {
		return "barcode_equivalence_class_set";
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public String getSequence() {
		return null;
	}

	public Barcode toBarcode() {
		return new Barcode("NA", getId());
	}

	@Override
	public Map<String, ReadSequenceElement> sequenceToElement() {
		Map<String, ReadSequenceElement> rtrn = new HashMap<String, ReadSequenceElement>();
		for(BarcodeEquivalenceClass equivClass : equivClasses) {
			rtrn.putAll(equivClass.sequenceToElement());
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
