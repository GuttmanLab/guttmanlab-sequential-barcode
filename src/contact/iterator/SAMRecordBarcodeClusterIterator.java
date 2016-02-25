package contact.iterator;

import htsjdk.samtools.fork.SAMFileHeader.SortOrder;
import htsjdk.samtools.fork.SAMRecordIterator;
import htsjdk.samtools.fork.SamReader;
import htsjdk.samtools.fork.SamReaderFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;

import net.sf.samtools.util.CloseableIterator;
import programs.BarcodedBamWriter;
import util.SAMConversionUtil;

/**
 * Iterator that returns successive collections of records with the same barcode
 * Asserts that bam file is sorted by barcode attribute
 * @author prussell
 *
 */
public class SAMRecordBarcodeClusterIterator implements CloseableIterator<Collection<net.sf.samtools.SAMRecord>> {
	
	private SamReader reader;
	private SAMRecordIterator iter;
	private net.sf.samtools.SAMRecord prevRecord;
	private static Logger logger = Logger.getLogger(SAMRecordBarcodeClusterIterator.class.getName());
	private static int numFragmentsDone = 0;
	private static int numClustersDone = 0;
	
	public SAMRecordBarcodeClusterIterator(String bamFile) {
		reader = SamReaderFactory.makeDefault().open(new File(bamFile));
		iter = reader.iterator();
		if(!iter.hasNext()) {
			throw new IllegalArgumentException("Iterator is empty for bam file " + bamFile);
		}
		iter.assertSorted(SortOrder.tagXB);
		prevRecord = SAMConversionUtil.fromForkSAMFragment(iter.next());
		numFragmentsDone++;
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public Collection<net.sf.samtools.SAMRecord> next() {
		numClustersDone++;
		if(numClustersDone % 1000000 == 0) {
			logger.info("Finished " + numClustersDone + " clusters");
		}
		Collection<net.sf.samtools.SAMRecord> rtrn = new ArrayList<net.sf.samtools.SAMRecord>();
		rtrn.add(prevRecord);
		String barcode = (String) prevRecord.getAttribute(BarcodedBamWriter.BARCODES_SAM_TAG);
		while(iter.hasNext()) {
			htsjdk.samtools.fork.SAMRecord next = iter.next();
			numFragmentsDone++;
			if(numFragmentsDone % 10000000 == 0) {
				logger.info("Finished " + numFragmentsDone + " SAM records");
			}
			prevRecord = SAMConversionUtil.fromForkSAMFragment(next);
			if(next.getAttribute(BarcodedBamWriter.BARCODES_SAM_TAG).equals(barcode)) {rtrn.add(SAMConversionUtil.fromForkSAMFragment(next));}
			else break;
		}
		return rtrn;
	}

	@Override
	public void close() {
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		iter.close();
	}

}
