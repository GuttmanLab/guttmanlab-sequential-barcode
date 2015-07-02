package readelement;

public abstract class AbstractReadSequenceElement implements ReadSequenceElement {

	@Override
	public int firstMatch(String s) {
		for(int i = 0; i < s.length() - getLength() + 1; i++) {
			if(matchesSubstringNoGaps(s,i)) {
				return i;
			}
		}
		return -1;
	}
	
	@Override
	public MatchedElement matchedElement(String s, int startPosOnString) {
		return matchedElement(s.substring(startPosOnString));
	}

	
}
