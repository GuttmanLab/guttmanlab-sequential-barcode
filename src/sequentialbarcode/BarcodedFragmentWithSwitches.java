package sequentialbarcode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import matcher.GenericElementMatcher;
import readelement.FixedSequence;
import readelement.ReadSequenceElement;
import readelement.Switch;
import readlayout.ReadLayout;

/**
 * A barcoded fragment containing switches, i.e. blocks of sequence that have several options and indicate something about the fragment
 * @author prussell
 *
 */
public class BarcodedFragmentWithSwitches extends BarcodedFragmentImpl {
	
	private Map<Switch, List<FixedSequence>> switchValues;
	
	/**
	 * @param fragmentId Fragment ID
	 * @param read1seq Read1 sequence
	 * @param read2seq Read2 sequence
	 * @param layoutRead1 Read1 layout or null if not specified
	 * @param layoutRead2 Read2 layout or null if not specified
	 */
	public BarcodedFragmentWithSwitches(String fragmentId, String read1seq, String read2seq, ReadLayout layoutRead1, ReadLayout layoutRead2) {
		super(fragmentId, read1seq, read2seq, layoutRead1, layoutRead2);
	}
	
	/**
	 * @return Map of switch to list of its values in the fragment
	 */
	public Map<Switch, List<FixedSequence>> getSwitchValues() {
		if(switchValues == null) {
			findSwitches();
		}
		return switchValues;
	}
	
	private void findSwitches() {
		switchValues = new HashMap<Switch, List<FixedSequence>>();
			if(read1layout != null && read1sequence != null) {
				List<List<ReadSequenceElement>> read1elements = new GenericElementMatcher(read1layout, read1sequence).getMatchedElements();
				if(read1elements != null) {
					for(int i = 0; i < read1elements.size(); i++) {
						ReadSequenceElement parentElement = read1layout.getElements().get(i);
						if(parentElement.getClass().equals(Switch.class)) {
							for(ReadSequenceElement elt : read1elements.get(i)) {
								if(!switchValues.containsKey(parentElement)) {
									switchValues.put((Switch) parentElement, new ArrayList<FixedSequence>());
								}
								switchValues.get(parentElement).add((FixedSequence) elt);
							}
							continue;
						}
					}
				}
				read1elements = null;
			}
			if(read2layout != null && read2sequence != null) {
				List<List<ReadSequenceElement>> read2elements = new GenericElementMatcher(read2layout, read2sequence).getMatchedElements();
				if(read2elements != null) {
					for(int i = 0; i < read2elements.size(); i++) {
						ReadSequenceElement parentElement = read2layout.getElements().get(i);
						if(parentElement.getClass().equals(Switch.class)) {
							for(ReadSequenceElement elt : read2elements.get(i)) {
								if(!switchValues.containsKey(parentElement)) {
									switchValues.put((Switch) parentElement, new ArrayList<FixedSequence>());
								}
								switchValues.get(parentElement).add((FixedSequence) elt);
							}
							continue;
						}
					}
				}
				read2elements = null;
			}
	}

	
}
