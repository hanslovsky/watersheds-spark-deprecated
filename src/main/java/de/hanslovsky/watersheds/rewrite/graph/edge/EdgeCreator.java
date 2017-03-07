package de.hanslovsky.watersheds.rewrite.graph.edge;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import gnu.trove.list.array.TDoubleArrayList;

public interface EdgeCreator extends EdgeDataSize
{

	public int create( final Edge e, final double weight, final double affinity, final long from, final long to, final long multiplicity );


	public default Edge edge( final TDoubleArrayList data )
	{
		return new Edge( data, dataSize() );
	}

	public static class SerializableCreator implements EdgeCreator, Serializable
	{

		@Override
		public int create( final Edge e, final double weight, final double affinity, final long from, final long to, final long multiplicity )
		{
			return e.add( weight, affinity, from, to, multiplicity );
		}

	}

	public static class AffinityHistogram implements EdgeCreator, Serializable
	{

		public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
		{
			LOG.setLevel( Level.INFO );
		}

		private final int nBins;

		private final double min;

		private final double max;

		private final double binWidth;

		public AffinityHistogram( final int nBins, final double min, final double max )
		{
			super();
			this.nBins = nBins;
			this.min = min;
			this.max = max;
			this.binWidth = ( max - min ) / nBins;
		}

		@Override
		public int create( final Edge e, final double weight, final double affinity, final long from, final long to, final long multiplicity )
		{
			// first entry: number of
			assert e.getDataSize() == nBins + 1;

			final int bin = ( int ) ( ( affinity - min ) / binWidth );
			LOG.trace( "Creating edge: " + e.getDataSize() + " " + nBins + " " + bin );

			final int index = e.add( weight, affinity, from, to, multiplicity, IntStream.range( 0, nBins + 1 ).mapToDouble( i -> Edge.ltd( i == 0 ? 1 : i == bin + 1 ? 1 : 0 ) ) );

			e.setIndex( index );
			LOG.trace( "Created edge with count: " + Edge.dtl( e.getData( 0 ) ) );

			if ( Edge.dtl( e.getData( 0 ) ) == 0 )
				throw new RuntimeException( "WAAAT??" );

			return index;
		}

		@Override
		public int dataSize()
		{
			return this.nBins + 1;
		}

	}

}
