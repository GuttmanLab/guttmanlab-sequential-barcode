package matcher;

import readelement.MatchedElement;
import readelement.ReadSequenceElement;
import readlayout.ReadLayout;

/**
 * Use Smith-Waterman to pre-cache matches for sequence elements within the read
 * @author prussell
 *
 */
public class SmithWatermanMatcher extends GenericElementMatcher {

	/**
	 * @param layout Read layout
	 * @param readSeq Read sequence
	 */
	public SmithWatermanMatcher(ReadLayout layout, String readSeq) {
		super(layout, readSeq);
	}

	@Override
	public MatchedElement getMatchedElement(ReadSequenceElement toMatch, String readSequence, int startPosOnRead) {
		return null;
		//TODO
	}

}
