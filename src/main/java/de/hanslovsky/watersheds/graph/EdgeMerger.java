package de.hanslovsky.watersheds.graph;

import java.io.Serializable;

public interface EdgeMerger extends Serializable
{
	// writes into e2
	Edge merge( final Edge source, final Edge target );
//		default Edge merge( final Edge e1, final Edge e2 )
//		{
//			final double w1 = e1.weight();
//			final double w2 = e2.weight();
//			if ( w1 < w2 )
//			{
//				e2.weight( w1 );
//				e2.affinity( e1.affinity() );
//			}
//
//			return e2;
//		}
}