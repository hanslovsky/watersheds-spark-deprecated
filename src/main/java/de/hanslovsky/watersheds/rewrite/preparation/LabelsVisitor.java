package de.hanslovsky.watersheds.rewrite.preparation;

import java.io.Serializable;

import de.hanslovsky.watersheds.rewrite.util.HashableLongArray;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.integer.LongType;

public interface LabelsVisitor extends Serializable
{
	public void act( HashableLongArray t, RandomAccessibleInterval< LongType > labels );
}