package contact.iterator;

import java.util.List;
import java.util.function.Predicate;

import guttmanlab.core.coordinatespace.CoordinateSpace;
import htsjdk.samtools.fork.SAMRecord;
import net.sf.samtools.util.CloseableIterator;
import contact.SAMFragmentCluster;

public class SAMFragmentClusterIterator implements CloseableIterator<SAMFragmentCluster> {
	
	private SAMRecordBarcodeClusterIterator iter;
	private CoordinateSpace coordSpace;
	
	/**
	 * @param bamFile Bam file
	 * @param coordSpace Coordinate space
	 * @param requiredConditions Collection of predicates for filtered SAMRecord iterator. SAMRecords for which
	 * any predicate evaluates to false will not be included
	 */
	public SAMFragmentClusterIterator(String bamFile, CoordinateSpace coordSpace, List<Predicate<SAMRecord>> requiredConditions) {
		this.coordSpace = coordSpace;
		iter = new SAMRecordBarcodeClusterIterator(bamFile, requiredConditions);
	}
	
	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public SAMFragmentCluster next() {
		return new SAMFragmentCluster(iter.next(), coordSpace);
	}

	@Override
	public void close() {
		iter.close();
	}
	
	
	
}
