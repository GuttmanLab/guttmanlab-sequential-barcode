package matcher;

import java.util.ArrayList;
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
		super(layout, readSeq, true);
	}

	@Override
	public MatchedElement getMatchedElement(ReadSequenceElement toMatch, int startPosOnRead) {
		//if(!matches.containsKey(toMatch)) return null;
		//if(!matches.get(toMatch).containsKey(Integer.valueOf(startPosOnRead))) return null;
		try {
			return matches.get(toMatch).get(Integer.valueOf(startPosOnRead));
		} catch(NullPointerException e) {
			return null;
		}
	}
	
	@Override
	public void cacheMatches() {
		matches = new HashMap<ReadSequenceElement, Map<Integer, MatchedElement>>();
		try {
			for(ReadSequenceElement element : readLayout.getElements()) {
				Collection<MatchedElement> matchLocations = matchLocations(element);
				if(matchLocations != null) {
					Map<Integer, MatchedElement> thisEltMatches = new HashMap<Integer, MatchedElement>();
					for(MatchedElement match : matchLocations) {
						thisEltMatches.put(Integer.valueOf(match.getMatchStartPosOnRead()), match);
					}
					matches.put(element, thisEltMatches);
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
	private Collection<MatchedElement> matchLocations(ReadSequenceElement element) {
		Map<String, ReadSequenceElement> seqs = readLayout.getSubElementsByRepresentative(element);
		Collection<MatchedElement> rtrn = new ArrayList<MatchedElement>();
		int lev = element.maxLevenshteinDist();
		int lengthOnRead = element.getLength() - lev; // TODO this is wrong! Just a placeholder!
		for(String seq : seqs.keySet()) {
			Bitap bitap = new Bitap(seq, readSequence, alphabet);
			List<Integer> matches = bitap.wuManber(lev);
			ReadSequenceElement seqElt = seqs.get(seq);
			for(Integer start : matches) {
				MatchedElement match = new MatchedElement(seqElt, start, lengthOnRead);
				rtrn.add(match); // TODO what to do if multiple elements match at same position?
			}
		}
		return rtrn;
	}

}


