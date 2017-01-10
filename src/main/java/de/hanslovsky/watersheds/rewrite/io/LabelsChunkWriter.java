package de.hanslovsky.watersheds.rewrite.io;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.util.HashableLongArray;
import de.hanslovsky.watersheds.rewrite.util.Util;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.LongType;
import scala.Tuple2;

public class LabelsChunkWriter implements
PairFunction< Tuple2< HashableLongArray, long[] >, HashableLongArray, long[] >
{

	private static final long serialVersionUID = 7110087147446657591L;

	private final FileWriter< LongType > writer;

	private final long[] dims;

	private final int[] intervalDims;

	public LabelsChunkWriter( final FileWriter< LongType > writer, final long[] dims, final int[] intervalDims )
	{
		super();
		this.writer = writer;
		this.dims = dims;
		this.intervalDims = intervalDims;
	}

	@Override
	public Tuple2< HashableLongArray, long[] >
	call( final Tuple2< HashableLongArray, long[] > t ) throws Exception
	{
		final long[] offset = t._1().getData();
		final long[] chunkDims = Util.getCurrentChunkDimensions( offset, dims, intervalDims );
		writer.write( offset, chunkDims, ArrayImgs.longs( t._2(), chunkDims ) );
		return t;
	}

}
