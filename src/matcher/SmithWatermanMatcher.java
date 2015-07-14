package matcher;

import guttmanlab.core.alignment.SmithWatermanAlignment;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import readelement.MatchedElement;
import readelement.ReadSequenceElement;
import readlayout.ReadLayout;

/**
 * Use Smith-Waterman to pre-cache matches for sequence elements within the read
 * @author prussell
 *
 */
public class SmithWatermanMatcher extends GenericElementMatcher {
	
	public static Logger logger = Logger.getLogger(SmithWatermanMatcher.class.getName());
	private Map<ReadSequenceElement, Map<Integer, MatchedElement>> matches;
	
	/**
	 * @param layout Read layout
	 * @param readSeq Read sequence
	 */
	public SmithWatermanMatcher(ReadLayout layout, String readSeq) {
		super(layout, readSeq);
	}

	@Override
	public MatchedElement getMatchedElement(ReadSequenceElement toMatch, int startPosOnRead) {
		if(!matches.containsKey(toMatch)) return null;
		if(!matches.get(toMatch).containsKey(Integer.valueOf(startPosOnRead))) return null;
		return matches.get(toMatch).get(Integer.valueOf(startPosOnRead));
	}
	
	@Override
	public void cacheMatches() {
		matches = new HashMap<ReadSequenceElement, Map<Integer, MatchedElement>>();
		for(ReadSequenceElement element : readLayout.getElements()) {
			Map<Integer, MatchedElement> matchLocations = matchLocations(element);
			if(matchLocations != null) {
				matches.put(element, matchLocations);
			}
		}
	}
	
	/**
	 * Match a read element to the read and get match locations
	 * @param element The element to match
	 * @return Map of first match position to matched element, or null if no match
	 */
	private Map<Integer, MatchedElement> matchLocations(ReadSequenceElement element) {
		Map<String, ReadSequenceElement> seqs = element.sequenceToElement();
		Map<Integer, MatchedElement> rtrn = new HashMap<Integer, MatchedElement>();
		int minMatches = element.minMatch();
		for(String seq : seqs.keySet()) {
			Collection<jaligner.Alignment> aligns = SmithWatermanAlignment.getAllAlignments(seq, readSequence, minMatches);
			for(jaligner.Alignment align : aligns) {
				int startOnRead = align.getStart2();
				int lengthOnRead = align.getNumberOfMatches() + align.getGaps1(); //TODO is this right?
				MatchedElement match = new MatchedElement(seqs.get(seq), lengthOnRead);
				rtrn.put(Integer.valueOf(startOnRead), match); //TODO what to do if multiple elements match at same position?
			}
		}
		return rtrn;
	}

}


