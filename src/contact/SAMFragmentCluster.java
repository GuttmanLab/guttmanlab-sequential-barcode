package contact;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;

import net.sf.samtools.SAMRecord;
import guttmanlab.core.annotation.SAMFragment;
import guttmanlab.core.annotationcollection.FeatureCollection;
import guttmanlab.core.coordinatespace.CoordinateSpace;

/**
 * Fragment cluster where the underlying annotations are SAMFragments and the annotation collection is a FeatureCollection
 * @author prussell
 *
 */
public class SAMFragmentCluster implements FragmentCluster<SAMFragment, FeatureCollection<SAMFragment>> {
	
	private FeatureCollection<SAMFragment> locations;
	private BarcodeSequence barcodes;
	
	/**
	 * @param samRecords Collection of SAMRecords to initialize the set of mapped locations
	 * @param coordSpace Coordinate space for the mappings
	 */
	public SAMFragmentCluster(Collection<SAMRecord> samRecords, CoordinateSpace coordSpace) {
		locations = new FeatureCollection<SAMFragment>(coordSpace);
		Iterator<SAMRecord> iter = samRecords.iterator();
		if(!iter.hasNext()) {
			throw new IllegalArgumentException("Collection of SAM records is empty");
		}
		SAMRecord first = iter.next();
		locations.add(new SAMFragment(first));
		barcodes = BarcodeSequence.fromSamRecord(first);
		while(iter.hasNext()) {
			SAMRecord next = iter.next();
			if(!BarcodeSequence.fromSamRecord(next).equals(barcodes)) {
				throw new IllegalArgumentException("Records have different barcodes: " + 
						first.getReadName() + ":" + barcodes + ", " + 
						next.getReadName() + ":" + BarcodeSequence.fromSamRecord(next).toString());
			}
			locations.add(new SAMFragment(next));
		}
	}
	
	@Override
	public BarcodeSequence getBarcodes() {
		return barcodes;
	}

	@Override
	public FeatureCollection<SAMFragment> getLocations() {
		return locations;
	}

	@Override
	public void addLocation(SAMFragment region) {
		locations.add(region);
	}

	@Override
	public String toString(Function<FragmentCluster<SAMFragment, FeatureCollection<SAMFragment>>, String> function) {
		return function.apply(this);
	}

}
