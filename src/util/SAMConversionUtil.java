package util;

import java.util.List;

public class SAMConversionUtil {
	
	/**
	 * Convert SAMRecord from pamelarussell fork to net.sf.samtools jar
	 * @param record Record to convert
	 * @return Converted record
	 */
	public static final net.sf.samtools.SAMRecord fromForkSAMFragment(htsjdk.samtools.fork.SAMRecord record) {
		net.sf.samtools.SAMRecord rtrn = new net.sf.samtools.SAMRecord(null);
		List<htsjdk.samtools.fork.SAMRecord.SAMTagAndValue> attributes = record.getAttributes();
		for(htsjdk.samtools.fork.SAMRecord.SAMTagAndValue tagAndValue : attributes) {
			rtrn.setAttribute(tagAndValue.tag, tagAndValue.value);
		}
		rtrn.setAlignmentStart(record.getAlignmentStart());
		rtrn.setBaseQualities(record.getBaseQualities());
		rtrn.setCigarString(record.getCigarString());
		rtrn.setDuplicateReadFlag(record.getDuplicateReadFlag());
		rtrn.setFlags(record.getFlags());
		rtrn.setInferredInsertSize(record.getInferredInsertSize());
		rtrn.setMappingQuality(record.getMappingQuality());
		rtrn.setMateAlignmentStart(record.getMateAlignmentStart());
		rtrn.setMateReferenceName(record.getMateReferenceName());
		rtrn.setReadBases(record.getReadBases());
		rtrn.setReadName(record.getReadName());
		rtrn.setReferenceName(record.getReferenceName());
		return rtrn;
	}
	
}
