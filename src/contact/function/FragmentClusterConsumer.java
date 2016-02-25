package contact.function;

import guttmanlab.core.annotation.Annotation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import contact.FragmentCluster;
import contact.order.StartPositionComparator;

/**
 * Functions that modify a fragment cluster
 * @author prussell
 *
 */
public final class FragmentClusterConsumer {
	
	// Prevent instantiation
	private FragmentClusterConsumer() {}
	
	/**
	 * Get a function that will modify a fragment cluster to ensure no mappings are within the specified distance of each other
	 * The implementation is not specified and makes no guarantees about which mappings will be removed by the function
	 * @param minDist Min allowable distance between mappings; function ensures removal of mappings within this distance of each other
	 * @return The function
	 */
	public static <T extends Annotation, S extends Collection<T>> Consumer<FragmentCluster<T, S>> minDistanceFilter(int minDist) {
		
		Consumer<FragmentCluster<T, S>> rtrn = new Consumer<FragmentCluster<T, S>>() {

			@Override
			public void accept(FragmentCluster<T, S> fragmentCluster) {
				S locations = fragmentCluster.getLocations();
				// Sort the mapped locations by start position
				List<T> locationsAsList = new ArrayList<T>();
				Collections.sort(locationsAsList, new StartPositionComparator());
				Iterator<T> sortedIter = locationsAsList.iterator();
				T prev = sortedIter.next();
				while(sortedIter.hasNext()) {
					T next = sortedIter.next();
					if(next.getReferenceName().equals(prev.getReferenceName()) 
							&& next.getReferenceStartPosition() - prev.getReferenceStartPosition() < minDist) {
						// Remove last element examined
						sortedIter.remove();
						continue;
					}
					prev = next;
				}
				// Replace locations in the fragment cluster by the filtered ones
				locations.clear();
				locations.addAll(locationsAsList);
			}
			
		};
		
		return rtrn;
		
	}
	
}
