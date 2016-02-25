package contact.order;

import guttmanlab.core.annotation.Annotation;

import java.util.Comparator;

/**
 * A comparator that compares Annotations by their chromosome name and then start position only
 * @author prussell
 *
 */
public class StartPositionComparator implements Comparator<Annotation> {

	@Override
	public int compare(Annotation region1, Annotation region2) {
		int chrCompare = region1.getReferenceName().compareTo(region2.getReferenceName());
		if(chrCompare != 0) return chrCompare;
		return region1.getReferenceStartPosition() - region2.getReferenceStartPosition();
	}

}
