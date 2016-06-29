package util;

import guttmanlab.core.coordinatespace.CoordinateSpace;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMRecordIterator;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

/**
 * Get mapping counts in genome wide bins
 * @author prussell
 *
 */
public class BinCounts {
	
	/**
	 * A single bin
	 * @author prussell
	 *
	 */
	private static final class Bin implements Comparable<Bin> {
		
		private String ref;
		private int start;
		private int end;
		
		public Bin(String ref, int start, int end) {
			this.ref = ref;
			this.start = start;
			this.end = end;
		}

		@Override
		public int compareTo(Bin o) {
			int r = ref.compareTo(o.ref);
			if(r != 0) return r;
			int s = start - o.start;
			if(s != 0) return s;
			return end - o.end;
		}
		
		@Override
		public String toString() {
			return ref + ":" + start + "-" + end;
		}
		
		@Override
		public boolean equals(Object o) {
			if(!o.getClass().equals(Bin.class)) return false;
			Bin b = (Bin) o;
			return ref.equals(b.ref) && start == b.start && end == b.end;
		}
		
		@Override
		public int hashCode() {
			return toString().hashCode();
		}
		
	}
	
	private static Logger logger = Logger.getLogger(BinCounts.class.getName());
	private Map<Bin, Integer> counts;
	
	/**
	 * Create the bin counts
	 * @param bam Bam file
	 * @param genome Coordinate space
	 * @param binSize Bin size
	 */
	public BinCounts(File bam, CoordinateSpace genome, int binSize) {
		makeCounts(bam, genome, binSize);
	}
	
	/**
	 * Make the counts
	 * @param bam Bam file
	 * @param genome Coordinate space
	 * @param binSize Bin size
	 */
	private void makeCounts(File bam, CoordinateSpace genome, int binSize) {
		logger.info("Making counts for file " + bam.getAbsolutePath() + " with bin size " + binSize + "...");
		SamReader reader = SamReaderFactory.makeDefault().open(bam);
		counts = new TreeMap<Bin, Integer>();
		for(String ref : genome.getRefSeqLengths().keySet()) {
			logger.info(ref);
			int start = 0;
			while(start <= genome.getRefSeqLengths().get(ref).intValue() - binSize) {
				int end = start + binSize;
				SAMRecordIterator iter = reader.query(ref, start, end, false);
				int count = 0;
				while(iter.hasNext()) {
					@SuppressWarnings("unused")
					SAMRecord r = iter.next();
					count++;
				}
				iter.close();
				Bin bin = new Bin(ref, start, end);
				counts.put(bin, Integer.valueOf(count));
				start += binSize;
			}
		}
	}
	
	/**
	 * Write bin counts to a file
	 * @param out Output file
	 */
	public void writeTable(File out) {
		try {
			FileWriter w = new FileWriter(out);
			for(Bin bin : counts.keySet()) {
				w.write(bin.toString() + "\t" + counts.get(bin) + "\n");
			}
			w.close();
		} catch(IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	/**
	 * Write bin counts to a table
	 * @param bamFile Bam file
	 * @param genome Coordinate space
	 * @param binSize Bin size
	 * @param output Output file
	 */
	public static void writeCounts(File bamFile, CoordinateSpace genome, int binSize, File output) {
		BinCounts bc = new BinCounts(bamFile, genome, binSize);
		bc.writeTable(output);
	}
	
	
	
	
}
