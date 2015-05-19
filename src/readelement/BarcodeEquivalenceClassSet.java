package readelement;

import java.util.Collection;

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
	public boolean matchesSubstringOf(String s, int startOnString) {
		for(BarcodeEquivalenceClass ec : equivClasses) {
			if(ec.matchesSubstringOf(s, startOnString)) return true;
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
	public String matchedElementSequence(String s) {
		return matchedElement(s).matchedElementSequence(s);
	}

	@Override
	public ReadSequenceElement matchedElement(String s) {
		for(BarcodeEquivalenceClass ec : equivClasses) {
			if(ec.matchesFullString(s)) {
				return ec;
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

}
