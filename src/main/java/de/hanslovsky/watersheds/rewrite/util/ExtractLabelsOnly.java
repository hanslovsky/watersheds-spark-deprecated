package de.hanslovsky.watersheds.rewrite.util;

import java.util.Arrays;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.Cursor;
import net.imglib2.IterableInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.LongArray;
import net.imglib2.type.numeric.integer.LongType;
import net.imglib2.view.Views;
import scala.Tuple2;
import scala.Tuple3;

public class ExtractLabelsOnly< K > implements PairFunction< Tuple2< K, Tuple3< long[], float[], TLongLongHashMap > >, K, long[] >
{

	private final Broadcast< long[] > dimsIntervalBC;

	public ExtractLabelsOnly( final Broadcast< long[] > dimsIntervalBC )
	{
		super();
		this.dimsIntervalBC = dimsIntervalBC;
	}

	@Override
	public Tuple2< K, long[] > call( final Tuple2< K, Tuple3< long[], float[], TLongLongHashMap > > t ) throws Exception
	{
		final long[] dimsInterval = dimsIntervalBC.getValue();
		final ArrayImg< LongType, LongArray > src = ArrayImgs.longs( t._2()._1(), Arrays.stream( dimsInterval ).map( v -> v + 2 ).toArray() );
		final IterableInterval< LongType > cutOut = Views.flatIterable( Views.offsetInterval( src, Arrays.stream( new long[ src.numDimensions() ] ).map( v -> 1 ).toArray(), dimsInterval ) );
		final Cursor< LongType > c = cutOut.cursor();
		final long[] result = new long[ ( int ) cutOut.size() ];
		for ( int i = 0; c.hasNext(); ++i )
			result[ i ] = c.next().get();
		return new Tuple2<>( t._1(), result );
	}

}
