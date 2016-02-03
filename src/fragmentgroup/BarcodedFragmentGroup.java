package fragmentgroup;

import guttmanlab.core.annotation.Annotation;

import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;

import sequentialbarcode.BarcodeSequence;
import sequentialbarcode.BarcodedFragment;

/**
 * A collection of fragments sharing the same barcodes, storing read IDs and mapped locations
 * @author prussell
 *
 */
public final class BarcodedFragmentGroup implements FragmentGroup {
	
	private BarcodeSequence barcodes;
	private Collection<BarcodedFragment> fragments;
	
	/**
	 * @param barcodeSignature The shared barcodes
	 */
	public BarcodedFragmentGroup(BarcodeSequence barcodeSignature) {
		this(barcodeSignature, new TreeSet<BarcodedFragment>());
	}
	
	/**
	 * @param barcodeSignature The shared barcodes
	 * @param barcodedFragments Some fragments with the barcodes
	 */
	public BarcodedFragmentGroup(BarcodeSequence barcodeSignature, Collection<BarcodedFragment> barcodedFragments) {
		barcodes = barcodeSignature;
		fragments = new TreeSet<BarcodedFragment>();
		for(BarcodedFragment fragment : barcodedFragments) {
			addFragment(fragment);
		}
	}
	
	public void addFragment(BarcodedFragment fragment) {
		if(!fragment.getBarcodes().equals(barcodes)) {
			throw new IllegalArgumentException("New fragment must have barcodes " + barcodes.toString());
		}
		fragments.add(fragment);
	}
	
	/**
	 * Delimiter separating fragments when this is expressed as a SAM attribute
	 */
	private static String SAM_ATTRIBUTE_FRAGMENT_DELIM = ";";
	
	
	
	public BarcodeSequence getBarcodes() {
		return barcodes;
	}
	
	public String toSamAttributeString() {
		if(fragments.isEmpty()) {
			throw new IllegalStateException("Fragment set is empty");
		}
		Iterator<BarcodedFragment> iter = fragments.iterator();
		BarcodedFragment f = iter.next();
		String rtrn = f.getFullInfoString();
		while(iter.hasNext()) {
			BarcodedFragment fr = iter.next();
			rtrn += SAM_ATTRIBUTE_FRAGMENT_DELIM + fr.getFullInfoString();
		}
		return rtrn;
	}


	@Override
	public Collection<Annotation> getRegions() {
		Collection<Annotation> rtrn = new TreeSet<Annotation>();
		for(BarcodedFragment fragment : fragments) {
			rtrn.add(fragment.getMappedLocation());
		}
		return rtrn;
	}

}
