package de.hanslovsky.watersheds.rewrite.graph;

import java.io.Serializable;

public interface EdgeMerger extends Serializable
{
	Edge merge( final Edge source, final Edge target );

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

}
