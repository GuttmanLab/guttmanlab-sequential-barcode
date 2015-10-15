package matcher;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import bitap.Bitap;
import readelement.MatchedElement;
import readelement.ReadSequenceElement;
import readlayout.ReadLayout;

/**
 * Use Bitap algorithm to pre-cache matches for sequence elements within the read
 * @author prussell
 *
 */
public class BitapMatcher extends GenericElementMatcher {
	
	public static Logger logger = Logger.getLogger(BitapMatcher.class.getName());
	private Map<ReadSequenceElement, Map<Integer, MatchedElement>> matches;
	private static Character[] alphabet = {'A', 'C', 'G', 'T', 'N'};
	
	/**
	 * @param layout Read layout
	 * @param readSeq Read sequence
	 */
	public BitapMatcher(ReadLayout layout, String readSeq) {
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
		try {
			for(ReadSequenceElement element : readLayout.getElements()) {
				Map<Integer, MatchedElement> matchLocations = matchLocations(element);
				if(matchLocations != null) {
					matches.put(element, matchLocations);
				}
			}
		} catch(NullPointerException e) {
			logger.info("Caught null pointer exception on read " + readSequence);
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
		int lev = element.maxLevenshteinDist();
		for(String seq : seqs.keySet()) {
			Bitap bitap = new Bitap(seq, readSequence, alphabet);
			List<Integer> matches = bitap.wuManber(lev);
			for(Integer start : matches) {
				int lengthOnRead = element.getLength() - lev; // TODO this is wrong! Just a placeholder!
				MatchedElement match = new MatchedElement(seqs.get(seq), lengthOnRead);
				rtrn.put(start, match); // TODO what to do if multiple elements match at same position?
			}
		}
		return rtrn;
	}

}


