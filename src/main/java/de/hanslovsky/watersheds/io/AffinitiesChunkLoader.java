package de.hanslovsky.watersheds.io;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.HashableLongArray;
import de.hanslovsky.watersheds.Util;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.util.Intervals;
import scala.Tuple2;

public class AffinitiesChunkLoader implements
PairFunction< HashableLongArray, HashableLongArray, float[] >
{

	private static final long serialVersionUID = 1L;

	private final FileOpener< FloatType > opener;

	private final long[] dims;

	private final int[] intervalDims;

	public AffinitiesChunkLoader( final FileOpener< FloatType > opener, final long[] dims, final int[] intervalDims )
	{
		super();
		this.opener = opener;
		this.dims = dims;
		this.intervalDims = intervalDims;
	}

	@Override
	public Tuple2< HashableLongArray, float[] > call( final HashableLongArray t ) throws Exception
	{

		final int dimensionality = t.getData().length;
		final long[] o = new long[ dimensionality + 1 ];
		System.arraycopy( t.getData(), 0, o, 0, dimensionality );
		o[ dimensionality ] = 0;
		final long[] currentDims = Util.getCurrentChunkDimensions( o, dims, intervalDims );
		final long nElements = Intervals.numElements( currentDims );
		final float[] store = new float[ ( int ) nElements ];
		opener.open( o, currentDims, ArrayImgs.floats( store, currentDims ) );
		return new Tuple2<>( t, store );
	}

}
