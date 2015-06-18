package readlayout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import org.apache.log4j.Logger;

import readelement.ReadSequenceElement;

import com.sleepycat.persist.model.Persistent;

/**
 * A sequence of read elements that should be present in a read
 * Can have other stuff between them
 * @author prussell
 *
 */
@Persistent
public class ReadLayout {
	
	private ArrayList<ReadSequenceElement> elements;
	private int readLen;
	
	/*
	 * The length of the part of the read up to the end of the last matched element
	 * Includes positions before and between elements if allowed
	 * This is not a constant; it changes every time we match a new read sequence
	 */
	private int totalLengthMatchedEltSection;
	
	public static Logger logger = Logger.getLogger(ReadLayout.class.getName());
	
	/**
	 * For Berkeley DB only
	 * Do not use this constructor
	 */
	public ReadLayout() {}
	
	/**
	 * @param elementSequence Sequence of elements expected in each read
	 * @param readLength Read length
	 */
	public ReadLayout(ArrayList<ReadSequenceElement> elementSequence, int readLength) {
		int totalLen = 0;
		for(ReadSequenceElement elt : elementSequence) {
			totalLen += elt.getLength();
		}
		if(totalLen > readLength) {
			throw new IllegalArgumentException("Total length of read sequence elements (" + totalLen + ") must be at most declared read length (" + readLength + ").");
		}
		elements = elementSequence;
		readLen = readLength;
	}
	
	/**
	 * Get names of read elements expected in each read
	 * @return Ordered list of read element names
	 */
	public ArrayList<String> getElementNames() {
		ArrayList<String> rtrn = new ArrayList<String>();
		for(ReadSequenceElement elt : elements) {
			rtrn.add(elt.elementName());
		}
		return rtrn;
	}
	
	/**
	 * Get read elements expected in each read
	 * @return Ordered list of read elements
	 */
	public ArrayList<ReadSequenceElement> getElements() {
		return elements;
	}
	
	/**
	 * Get the length in the read of all the matched elements including positions before and between them
	 * i.e., the length of the read minus anything that comes after the last matched element
	 * @param readSequence Read sequence
	 * @return Total length of all matched elements and positions before and between them
	 * @throws IOException 
	 */
	public int matchedElementsLengthInRead(String readSequence) throws IOException {
		try {
			if(totalLengthMatchedEltSection == -1) {
				throw new IllegalArgumentException("Read does not match layout");
			}
			return totalLengthMatchedEltSection;
		} catch(NullPointerException e) {
			throw new IllegalStateException("Must call getMatchedElements first");
		}
	}
	
	/**
	 * For repeatable elements, save the first occurrences of their "next" element so can keep looking up until next element
	 * @param readSequence Read sequence to search for positions of "next" elements
	 * @return Map of element to position of its "next" element, or Integer.MAX_VALUE if no stop signal
	 */
	private Map<ReadSequenceElement, Integer> findStopSignalPositions(String readSequence) {
		Map<ReadSequenceElement, Integer> stopSignalPos = new HashMap<ReadSequenceElement, Integer>();
		for(ReadSequenceElement elt : elements) {
			if(elt.isRepeatable()) {
				// First check if element has a stop signal
				ReadSequenceElement stopSignal = elt.getStopSignalForRepeatable();
				if(stopSignal == null) {
					stopSignalPos.put(elt, Integer.MAX_VALUE);
					continue;
				}
				int posNext = stopSignal.firstMatch(readSequence); // EFFICIENCY make firstMatch more efficient
				if(posNext != -1) {
					stopSignalPos.put(elt, Integer.valueOf(posNext));
					logger.debug("STOP_SIGNAL\t for element " + elt.getId() + " is at position " + posNext);
				}
			}
		}
		return stopSignalPos;
	}
	
	
	public String toString() {
		Iterator<ReadSequenceElement> iter = elements.iterator();
		String rtrn = iter.next().elementName();
		while(iter.hasNext()) {
			rtrn += "_" + iter.next().elementName();
		}
		return rtrn;
	}
	
	
	public class ElementMatcher {
				
		private String readSequence;
		private int currStart;
		private ReadSequenceElement currElt;
		private int currEltIndex;
		private Map<ReadSequenceElement, Integer> stopSignalPos;
		private Iterator<ReadSequenceElement> elementIter;
		private ReadSequenceElement nextElt;
		private boolean[] found;
		
		/**
		 * @param readSeq Read sequence
		 */
		public ElementMatcher(String readSeq) {
			readSequence = readSeq;
			// For repeatable elements, save the first occurrences of their "next" element so can keep looking up until next element
			stopSignalPos = findStopSignalPositions(readSequence);
			// Look for all the elements in order; can have other stuff between them
			elementIter = elements.iterator();
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
			found = new boolean[elements.size()];
		}
		
		/**
		 * Check criteria that indicate there is no match for the layout:
		 * Read sequence is different from specified read length
		 * A value of true indicates that the finder should stop and return null
		 * @return True iff the criterion indicates there is no match
		 */
		private boolean stopNoMatchReadSeqLength() {
			if(readSequence.length() != readLen) {
				logger.debug("WRONG_LENGTH\tRead layout " + toString() + " does not match read " + readSequence + 
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
		
//		/**
//		 * Check criteria that indicate there is no match for the layout:
//		 * 
//		 * A value of true indicates that the finder should stop and return null
//		 * @return True iff the criterion indicates there is no match
//		 */
//		private boolean stopNoMatch() {
//
//		}
//		
//		/**
//		 * Check criteria that indicate there is no match for the layout:
//		 * 
//		 * A value of true indicates that the finder should stop and return null
//		 * @return True iff the criterion indicates there is no match
//		 */
//		private boolean stopNoMatch() {
//
//		}
		
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
		 * Get a list of matched elements in the read sequence
		 * Only returns an object if the whole layout matches the read sequence
		 * Each item on list corresponds to one read sequence element in the layout
		 * List item is an ordered list of matched elements for that read element
		 * If whole layout does not match read sequence, returns null
		 * @param readSequence Read sequence to search for matches to this layout
		 * @return List of lists of matched elements from this layout that appear in the read
		 * @throws IOException 
		 */
		public List<List<ReadSequenceElement>> getMatchedElements() {
			
			List<List<ReadSequenceElement>> rtrn = new ArrayList<List<ReadSequenceElement>>();
			for(int i = 0; i < elements.size(); i++) {
				rtrn.add(new ArrayList<ReadSequenceElement>());
			}
			
			if(stopNoMatchReadSeqLength()) return null; // Check that the read sequence has the read length required by this layout
			
			while(currStart < readLen) {
				if(currElt == null) { // We've reached the end of the set of elements
					totalLengthMatchedEltSection = currStart;
					return rtrn;
				}
				debugCurrPos();
				if(stopNoMatchEltsLeftOver()) return null; // Too far along in the read and have not found everything required
				if(currElt.isRepeatable() && nextElt != null) { // If current element is repeatable, look for next element at this position
					if(!(currStart + nextElt.getLength() > readLen)) {
						boolean lookNext = false;
						if(!stopSignalPos.containsKey(currElt)) lookNext = true;
						else if(stopSignalPos.get(currElt).intValue() == currStart) lookNext = true;
						if(lookNext) {
							debugLookingForNextElt();
							boolean nextMatches = nextElt.matchesSubstringOf(readSequence, currStart);
							if(stopNoMatchFoundNextBeforeCurrent(nextMatches)) return null; // Found next element before current element
							if(nextElt != null && nextMatches) {
								debugFoundNext();
								found[elements.indexOf(nextElt)] = true;
								rtrn.get(elements.indexOf(nextElt)).add(nextElt.matchedElement(readSequence.substring(currStart, currStart + nextElt.getLength())));
								debugNumMatches(rtrn.get(elements.indexOf(nextElt)).size());
								currStart += nextElt.getLength();
								if(!elementIter.hasNext()) { // We have found a match for the last element; return
									debugMatchedLayout();
									totalLengthMatchedEltSection = currStart; // Change the length of matched elements section
									return rtrn;
								} 
								currElt = elementIter.next(); // Now look for the element after "nextElt"
								currEltIndex++;
								debugNewCurrElt();
								if(elementIter.hasNext()) {
									nextElt = elementIter.next();
									debugNewNextElt(nextElt.getId());
								} else {
									debugNewNextElt("null");
									nextElt = null;
								}
								continue;
							}
						}
						debugNotLookingNextElt();
					}
				}
				// Look for current element
				if(currElt.matchesSubstringOf(readSequence, currStart)) { // Found an instance of current element
					debugMatchedCurrElt();
					found[currEltIndex] = true;
					rtrn.get(currEltIndex).add(currElt.matchedElement(readSequence.substring(currStart, currStart + currElt.getLength()))); // Add to return data structure
					debugNumMatches(rtrn.get(currEltIndex).size());
					currStart += currElt.getLength(); // Change current position to end of element
					// Now will look for the next element unless the current element is repeatable
					if(!currElt.isRepeatable()) {
						currElt = nextElt;
						currEltIndex++;
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
			return null;
		}

		
	}
	
	
	
}
