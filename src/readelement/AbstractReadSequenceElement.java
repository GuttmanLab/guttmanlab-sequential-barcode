package readelement;

public abstract class AbstractReadSequenceElement implements ReadSequenceElement {

	@Override
	public MatchedElement matchedElement(String s, int startPosOnString) {
		return matchedElement(s.substring(startPosOnString));
	}

	
}
