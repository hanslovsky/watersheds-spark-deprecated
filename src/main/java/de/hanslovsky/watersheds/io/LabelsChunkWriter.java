package de.hanslovsky.watersheds.io;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.Util;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.LongType;
import scala.Tuple2;
import scala.Tuple3;

public class LabelsChunkWriter implements
PairFunction< Tuple2< Tuple3< Long, Long, Long >, long[] >, Tuple3< Long, Long, Long >, long[] >
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
	public Tuple2< Tuple3< Long, Long, Long >, long[] >
	call( final Tuple2< Tuple3< Long, Long, Long >, long[] > t ) throws Exception
	{
		final long[] offset = new long[] { t._1()._1(), t._1()._2(), t._1()._3() };
		final long[] chunkDims = Util.getCurrentChunkDimensions( offset, dims, intervalDims );
		writer.write( offset, chunkDims, ArrayImgs.longs( t._2(), chunkDims ) );
		return t;
	}

}
