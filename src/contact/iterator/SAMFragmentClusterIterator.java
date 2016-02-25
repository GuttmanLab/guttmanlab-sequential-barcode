package contact.iterator;

import guttmanlab.core.coordinatespace.CoordinateSpace;

import net.sf.samtools.util.CloseableIterator;
import contact.SAMFragmentCluster;

public class SAMFragmentClusterIterator implements CloseableIterator<SAMFragmentCluster> {
	
	private SAMRecordBarcodeClusterIterator iter;
	private CoordinateSpace coordSpace;
	
	
	public SAMFragmentClusterIterator(String bamFile, CoordinateSpace coordSpace) {
		this.coordSpace = coordSpace;
		iter = new SAMRecordBarcodeClusterIterator(bamFile);
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
