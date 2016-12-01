package de.hanslovsky.watersheds;

import java.io.Serializable;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.type.numeric.integer.LongType;
import scala.Tuple3;

public interface LabelsVisitor extends Serializable
{
	public void act( Tuple3< Long, Long, Long > t, RandomAccessibleInterval< LongType > labels );
}