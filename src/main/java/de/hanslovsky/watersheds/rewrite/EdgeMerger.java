package de.hanslovsky.watersheds.rewrite;

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

}
