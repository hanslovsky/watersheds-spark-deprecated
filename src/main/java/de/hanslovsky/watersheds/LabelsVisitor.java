package de.hanslovsky.watersheds;

import java.io.Serializable;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.integer.LongType;

public interface LabelsVisitor extends Serializable
{
	public void act( HashableLongArray t, RandomAccessibleInterval< LongType > labels );
}