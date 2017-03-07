package de.hanslovsky.watersheds.rewrite.graph.edge;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public interface EdgeMerger extends Serializable, EdgeDataSize
{
	public Edge merge( final Edge source, final Edge target );

	public static class MAX_AFFINITY_MERGER implements EdgeMerger, Serializable
	{

		@Override
		public Edge merge( final Edge source, final Edge target )
		{
			target.affinity( Math.max( source.affinity(), target.affinity() ) );
			target.multiplicity( source.multiplicity() + target.multiplicity() );
			return target;
		}

	}

	public static class MIN_AFFINITY_MERGER implements EdgeMerger, Serializable
	{

		@Override
		public Edge merge( final Edge source, final Edge target )
		{
			target.affinity( Math.min( source.affinity(), target.affinity() ) );
			target.multiplicity( source.multiplicity() + target.multiplicity() );
			return target;
		}

	}

	public static class AVG_AFFINITY_MERGER implements EdgeMerger, Serializable
	{

		@Override
		public Edge merge( final Edge source, final Edge target )
		{
			final long m1 = source.multiplicity();
			final long m2 = target.multiplicity();
			final long m = m1 + m2;
			target.affinity( ( m1 * source.affinity() + m2 * target.affinity() ) / m );
			target.multiplicity( m );
			return target;
		}

	}

	public static class MEDIAN_AFFINITY_MERGER implements EdgeMerger, Serializable
	{

		public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
		{
			LOG.setLevel( Level.INFO );
		}

		private final int nBins;

		private final int dataSize;

		public MEDIAN_AFFINITY_MERGER( final int nBins )
		{
			super();
			this.nBins = nBins;
			this.dataSize = nBins + 1;
		}

		@Override
		public Edge merge( final Edge source, final Edge target )
		{
			LOG.trace( "Merging edges: " + source + " " + target + " (sizes: " + source.size() + " " + target.size() + ")" );

			final long m1 = source.multiplicity();
			final long m2 = target.multiplicity();
			final long m = m1 + m2;
			target.multiplicity( m );

			for ( int d = 0; d < dataSize; ++d )
			{
				final long d1 = Edge.dtl( source.getData( d ) );
				final long d2 = Edge.dtl( target.getData( d ) );
				if ( d == 0 && ( d1 == 0 || d2 == 0 ) )
					throw new RuntimeException( "SOMETHING WRONG HERE!!!!" + source + " " + target + " " + dataSize + " " + source.getDataSize() + " " + target.getDataSize() );
				target.setData( d, Edge.ltd( d1 + d2 ) );
			}

			return target;
		}

		@Override
		public int dataSize()
		{
			return dataSize;
		}

	}

}
