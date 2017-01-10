package de.hanslovsky.watersheds.rewrite.graph;

import java.io.Serializable;

public interface EdgeWeight
{

	public double weight( double affinity, long count1, long count2 );

	public static class FunkyWeight implements EdgeWeight, Serializable
	{
		@Override
		public double weight( final double affinity, final long c1, final long c2 )
		{
			return Math.min( c1, c2 ) * ( 1.0 - affinity );
		}
	}

}
