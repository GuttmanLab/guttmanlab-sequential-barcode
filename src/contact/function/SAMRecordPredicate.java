package contact.function;

import htsjdk.samtools.fork.SAMRecord;

import java.util.function.Predicate;

/**
 * Static classes representing SAMRecord predicates
 * @author prussell
 *
 */
public class SAMRecordPredicate {
	
	/**
	 * @return True if the record is marked as the primary mapping
	 */
	public static final class PrimaryMapping implements Predicate<SAMRecord> {
		
		@Override
		public boolean test(SAMRecord t) {
			return !t.getNotPrimaryAlignmentFlag();
		}
		
	}
	
}
