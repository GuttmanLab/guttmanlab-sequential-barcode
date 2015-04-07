package readelement;

public abstract class AbstractReadSequenceElement implements ReadSequenceElement {

	@Override
	public int firstMatch(String s) {
		for(int i = 0; i < s.length() - getMinLength() + 1; i++) {
			if(matchesSubstringOf(s,i)) {
				return i;
			}
		}
		return -1;
	}

}
