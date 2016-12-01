package de.hanslovsky.watersheds;

import java.util.Comparator;

import org.apache.spark.api.java.function.PairFunction;

import net.imglib2.util.IntervalIndexer;
import scala.Tuple2;
import scala.Tuple3;

public class Util
{
	public static long[] getCurrentChunkDimensions( final long[] offset, final long[] dims, final int[] intervalDims )
	{
		final long[] intervalDimensionsTruncated = new long[ dims.length ];
		for ( int d = 0; d < intervalDimensionsTruncated.length; ++d )
			intervalDimensionsTruncated[ d ] = Math.min(
					dims[ d ] - offset[ d ],
					intervalDims[ d ] );
		return intervalDimensionsTruncated;
	}

	public static long positionToIndex( final Tuple3< Long, Long, Long > pos, final long[] dim )
	{
		final long[] arr = new long[] { pos._1(), pos._2(), pos._3() };
		return IntervalIndexer.positionToIndex( arr, dim );
	}

	public static class KeyAndCountsComparator< V > implements
	Comparator< Tuple2< Tuple3< Long, Long, Long >, V > >
	{
		private final long[] dims;

		public KeyAndCountsComparator( final long[] dims )
		{
			super();
			this.dims = dims;
		}

		@Override
		public int compare( final Tuple2< Tuple3< Long, Long, Long >, V > o1, final Tuple2< Tuple3< Long, Long, Long >, V > o2 )
		{
			return Long.compare( positionToIndex( o1._1(), dims ), positionToIndex( o2._1(), dims ) );
		}
	}

	public static class DropSecondValue< K, V1, V2 > implements PairFunction< Tuple2< K, Tuple2< V1, V2 > >, K, V1 >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 3499313015214140909L;

		@Override
		public Tuple2< K, V1 > call( final Tuple2< K, Tuple2< V1, V2 > > t ) throws Exception
		{
			return new Tuple2<>( t._1(), t._2()._1() );
		}

	}
}
