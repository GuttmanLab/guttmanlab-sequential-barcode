package contact.function;

import guttmanlab.core.annotation.Annotation;

import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.function.Function;

import contact.FragmentCluster;

public final class FragmentClusterFunction {
	
	// Prevent instantiation
	private FragmentClusterFunction() {}
	
	/**
	 * Get a function that takes a fragment cluster and returns a tab delimited string representation
	 * The string returned is \<barcodes\> \<location1\> ... \<locationN\>
	 * Identical locations are only included once
	 * @return A function that produces this string representatin of a fragment cluster
	 */
	public static final <T extends Annotation, S extends Collection<T>> Function<FragmentCluster<T, S>, String> tabDelimitedString() {
		
		Function<FragmentCluster<T, S>, String> rtrn = new Function<FragmentCluster<T, S>, String>() {
			
			@Override
			public String apply(FragmentCluster<T, S> fragmentCluster) {
				StringBuilder sb = new StringBuilder(fragmentCluster.getBarcodes().toString());
				Iterator<T> iter = fragmentCluster.getLocations().iterator();
				Collection<String> locs = new TreeSet<String>();
				while(iter.hasNext()) {
					T location = iter.next();
					locs.add(location.getReferenceName() + ":" + location.getReferenceStartPosition());
				}
				for(String loc : locs) {
					sb.append("\t" + loc);
				}
				return sb.toString();
			}
			
		};
		
		return rtrn;
		
	}
	
}
