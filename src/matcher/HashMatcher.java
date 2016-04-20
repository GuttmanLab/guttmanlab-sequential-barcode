package matcher;

import org.apache.log4j.Logger;

import readelement.ReadSequenceElement;
import readlayout.ReadLayout;
import readlayout.ReadLayoutSequenceHash;

/**
 * Use a hash table to store mapping between imperfect sequences and the sequences they represent
 * DOES NOT SUPPORT INDELS
 * @author prussell
 *
 */
public final class HashMatcher extends GenericElementMatcher {
	
	private ReadLayoutSequenceHash hash;
	public static Logger logger = Logger.getLogger(HashMatcher.class.getName());
	
	/**
	 * @param layout Read layout
	 * @param readSeq Read sequence
	 */
	public HashMatcher(ReadLayout layout, String readSeq, ReadLayoutSequenceHash seqHash) {
		super(layout, readSeq, false);
		hash = seqHash;
		cacheAndMatch();
	}

	@Override
	public MatchedElement getMatchedElement(ReadSequenceElement toMatch, int startPosOnRead) {
		int len = toMatch.getLength();
		ReadSequenceElement bestMatch = hash.bestMatch(readSequence.substring(startPosOnRead, startPosOnRead + len), toMatch.maxLevenshteinDist());
		if(toMatch.sequenceToElement().values().contains(bestMatch)) {
			if(len != toMatch.getLength()) {
				throw new IllegalStateException("Length of query element must equal length of matched element");
			}
			logger.debug("MATCHED_ELEMENT\tfor " + toMatch.getId() + " " + toMatch.getSequence() + ": " + bestMatch.getId() + " " 
			+ bestMatch.getSequence() + " pos: " + startPosOnRead + " len: " + len);
			return new MatchedElement(bestMatch, startPosOnRead, len);
		}
		return null;
	}
	
}
