package matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import readelement.MatchedElement;
import readelement.ReadSequenceElement;
import readlayout.ReadLayout;

/**
 * Go across the read one position at a time and try to match elements of any type
 * @author prussell
 *
 */
public class GenericElementMatcher implements ElementMatcher {
	
	protected String readSequence;
	private int currStart;
	private ReadSequenceElement currElt;
	private int currEltIndex;
	private Map<ReadSequenceElement, Integer> stopSignalPos;
	private Iterator<ReadSequenceElement> elementIter;
	private ReadSequenceElement nextElt;
	private boolean[] found;
	private List<List<ReadSequenceElement>> matchedElements;
	protected ReadLayout readLayout;
	public static Logger logger = Logger.getLogger(GenericElementMatcher.class.getName());
	/*
	 * The length of the part of the read up to the end of the last matched element
	 * Includes positions before and between elements if allowed
	 */
	private int totalLengthMatchedEltSection;
	private int readLen;

	/**
	 * @param layout Read layout
	 * @param readSeq Read sequence
	 */
	public GenericElementMatcher(ReadLayout layout, String readSeq) {
		readLayout = layout;
		readSequence = readSeq;
		// For repeatable elements, save the first occurrences of their "next" element so can keep looking up until next element
		stopSignalPos = findStopSignalPositions();
		// Look for all the elements in order; can have other stuff between them
		elementIter = readLayout.getElements().iterator();
		readLen = layout.getReadLength();
		// Get the first element and look ahead to the next element
		// If current element is repeatable, will use next element to know when to stop looking for current element
		currElt = elementIter.next();
		currEltIndex = 0;
		nextElt = null;
		if(elementIter.hasNext()) {
			nextElt = elementIter.next();
		}
		currStart = 0;
		// Make sure all elements have been found at least once in the specified order
		found = new boolean[readLayout.getElements().size()];
		// Match elements
		cacheMatches();
		matchElements();
	}
	
	/**
	 * For repeatable elements, save the first occurrences of their "next" element so can keep looking up until next element
	 * @return Map of element to position of its "next" element, or Integer.MAX_VALUE if no stop signal
	 */
	private Map<ReadSequenceElement, Integer> findStopSignalPositions() {
		Map<ReadSequenceElement, Integer> stopSignalPos = new HashMap<ReadSequenceElement, Integer>();
		for(ReadSequenceElement elt : readLayout.getElements()) {
			if(elt.isRepeatable()) {
				// First check if element has a stop signal
				ReadSequenceElement stopSignal = elt.getStopSignalForRepeatable();
				if(stopSignal == null) {
					stopSignalPos.put(elt, Integer.MAX_VALUE);
					continue;
				}
				int posNext = stopSignal.firstMatch(readSequence);
				if(posNext != -1) {
					stopSignalPos.put(elt, Integer.valueOf(posNext));
					logger.debug("STOP_SIGNAL\t for element " + elt.getId() + " is at position " + posNext);
				}
			}
		}
		return stopSignalPos;
	}
	
	public int matchedElementsLengthInRead() {
		if(totalLengthMatchedEltSection == -1) {
			throw new IllegalArgumentException("Read does not match layout");
		}
		return totalLengthMatchedEltSection;
	}
	
	/**
	 * Check criteria that indicate there is no match for the layout:
	 * Read sequence is different from specified read length
	 * A value of true indicates that the finder should stop and return null
	 * @return True iff the criterion indicates there is no match
	 */
	private boolean stopNoMatchReadSeqLength() {
		if(readSequence.length() != readLen) {
			logger.debug("WRONG_LENGTH\tRead layout " + readLayout.toString() + " does not match read " + readSequence + 
					" because lengths are different: " + readLen + ", " + readSequence.length());
			return true;
		}
		return false;
	}
	
	/**
	 * Check criteria that indicate there is no match for the layout:
	 * Too far along in read and haven't found all elements
	 * A value of true indicates that the finder should stop and return null
	 * @return True iff the criterion indicates there is no match
	 */
	private boolean stopNoMatchEltsLeftOver() {
		if(currStart + currElt.getLength() > readLen) {
			logger.debug("NO_MATCH_FOR_LAYOUT\tNo match for element " + currElt.getId() + " in read " + readSequence);
			// Change the length of matched elements section
			totalLengthMatchedEltSection = -1;
			return true;
		}
		return false;
	}
	
	/**
	 * Check criteria that indicate there is no match for the layout:
	 * We found a match for next element before current element
	 * A value of true indicates that the finder should stop and return null
	 * @param nextMatches Next element matches at current position
	 * @return True iff the criterion indicates there is no match
	 */
	private boolean stopNoMatchFoundNextBeforeCurrent(boolean nextMatches) {
		if(nextMatches && !found[currEltIndex]) {
			// Next element was found before any instance of current element
			logger.debug("FOUND_NEXT_BEFORE_CURRENT\tFound match for next element " + nextElt.getId() + " before any instance of " + currElt.getId());
			// Change the length of matched elements section
			totalLengthMatchedEltSection = -1;
			return true;
		}
		return false;
	}
	
	private void debugCurrPos() {
		logger.debug("");
		logger.debug("CURRENT_START\t" + currStart);
		logger.debug("CURRENT_ELEMENT\t" + currElt.getId());
		logger.debug("NEXT_ELEMENT\t" + (nextElt == null ? null : nextElt.getId()));
	}
	
	private void debugLookingForNextElt() {
		logger.debug("LOOKING_FOR_NEXT_ELT\tLooking for " + nextElt.getId() + " at position " + currStart);
	}
	
	private void debugFoundNext() {
		logger.debug("FOUND_NEXT_OK\tFound match for next element " + nextElt.getId() + " at start position " + currStart + " of read " + readSequence);
	}
	
	private void debugMatchedLayout() {
		logger.debug("MATCHED_LAYOUT\tFound match for entire read layout");
	}
	
	private void debugNumMatches(int size) {
		logger.debug("NUM_MATCHES\tThere are " + size + " matches for this element of length " + currElt.getLength() + ".");
	}
	
	private void debugNewCurrElt() {
		logger.debug("NEW_CURR_ELT\t" + currElt.getId());
	}
	
	private void debugNewNextElt(String id) {
		logger.debug("NEW_NEXT_ELT\t" + id);
	}
	
	private void debugNotLookingNextElt() {
		logger.debug("NOT_LOOKING_FOR_NEXT_ELT\tNot looking for " + nextElt.getId() + " at position " + currStart);
	}
	
	private void debugMatchedCurrElt() {
		logger.debug("MATCHED_CURRENT_ELEMENT\tFound match for element " + currElt.getId() + " at start position " + currStart + " of read " + readSequence);
	}
	
	private void debugNoNewCurrElt() {
		logger.debug("NO_NEW_CURR_ELT");
	}
	
	private void debugNoNewNextElt() {
		logger.debug("NO_NEW_NEXT_ELT");		
	}
	
	private void debugNoMatch() {
		logger.debug("No match for element " + currElt.getId() + " at start position " + currStart + " of read " + readSequence);
	}
	
	/**
	 * Whether to look for the next element now
	 * @return True iff should look for next element
	 */
	private boolean lookNext() {
		if(!stopSignalPos.containsKey(currElt)) return true;
		else if(stopSignalPos.get(currElt).intValue() == currStart) return true;
		return false;
	}
	
	/**
	 * Get the list of matched elements in the read sequence
	 * Each item on list corresponds to one read sequence element in the layout
	 * List item is an ordered list of matched elements for that read element
	 * If whole layout does not match read sequence, returns null
	 * @return List of matched elements or null if whole layout doesn't match
	 * @throws IOException 
	 */
	public List<List<ReadSequenceElement>> getMatchedElements() {
		return matchedElements;
	}
	
	
	/**
	 * Get the list of matched elements in the read sequence
	 * Each item on list corresponds to one read sequence element in the layout
	 * List item is an ordered list of matched elements for that read element
	 * If whole layout does not match read sequence, sets list to null
	 * @throws IOException 
	 */
	private void matchElements() {
		
		matchedElements = new ArrayList<List<ReadSequenceElement>>();
		for(int i = 0; i < readLayout.getElements().size(); i++) {matchedElements.add(new ArrayList<ReadSequenceElement>());}
		if(stopNoMatchReadSeqLength()) {matchedElements = null; return;} // Check that the read sequence has the read length required by this layout
		
		while(currStart < readLen) {
			if(currElt == null) {totalLengthMatchedEltSection = currStart; return;} // We've reached the end of the set of elements
			debugCurrPos();
			if(stopNoMatchEltsLeftOver()) {matchedElements = null; return;} // Too far along in the read and have not found everything required
			if(currElt.isRepeatable() && nextElt != null) { // If current element is repeatable, look for next element at this position
				if(!(currStart + nextElt.getLength() > readLen)) {
					if(lookNext()) {
						debugLookingForNextElt();
						MatchedElement nextMatch = getMatchedElement(nextElt, currStart);
						boolean nextMatches = nextMatch != null;
						if(stopNoMatchFoundNextBeforeCurrent(nextMatches)) {matchedElements = null; return;} // Found next element before current element
						if(nextMatches) {
							debugFoundNext();
							found[readLayout.getElements().indexOf(nextElt)] = true;
							matchedElements.get(readLayout.getElements().indexOf(nextElt)).add(nextMatch.getMatchedElement());
							debugNumMatches(matchedElements.get(readLayout.getElements().indexOf(nextElt)).size());
							currStart += nextMatch.getMatchLengthOnRead();
							if(!elementIter.hasNext()) { // We have found a match for the last element; return
								debugMatchedLayout();
								totalLengthMatchedEltSection = currStart; return; // Change the length of matched elements section
							} 
							currElt = elementIter.next(); // Now look for the element after "nextElt"
							currEltIndex++;
							debugNewCurrElt();
							if(elementIter.hasNext()) {nextElt = elementIter.next(); debugNewNextElt(nextElt.getId());}
							else {debugNewNextElt("null"); nextElt = null;}
							continue;
						}
					}
					debugNotLookingNextElt();
				}
			}
			// Look for current element
			MatchedElement currMatch = getMatchedElement(currElt, currStart);
			if(currMatch != null) { // Found an instance of current element
				debugMatchedCurrElt();
				found[currEltIndex] = true;
				matchedElements.get(currEltIndex).add(currMatch.getMatchedElement()); // Add to return data structure
				debugNumMatches(matchedElements.get(currEltIndex).size());
				currStart += currMatch.getMatchLengthOnRead(); // Change current position to end of element
				// Now will look for the next element unless the current element is repeatable
				if(!currElt.isRepeatable()) {
					currElt = nextElt; currEltIndex++;
					nextElt = elementIter.hasNext() ? elementIter.next() : null;
					if(currElt != null) debugNewCurrElt(); else debugNoNewCurrElt();
					if(nextElt != null) debugNewNextElt(nextElt.getId()); else debugNoNewNextElt();
				}
				continue;
			}
			debugNoMatch();
			currStart++;
		}
		totalLengthMatchedEltSection = -1; // Change the length of matched elements section
		matchedElements = null; return;
	}

	@Override
	public ReadLayout getReadLayout() {
		return readLayout;
	}

	@Override
	public String getOriginalSequence() {
		return readSequence;
	}

	@Override
	public MatchedElement getMatchedElement(ReadSequenceElement toMatch, int startPosOnRead) {
		return toMatch.matchedElement(readSequence, startPosOnRead);
	}

	@Override
	public void cacheMatches() {
		// Don't do anything
		return;
	}

	
	
}
