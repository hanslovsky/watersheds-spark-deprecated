package de.hanslovsky.watersheds.rewrite.util;

import java.io.Serializable;

import de.hanslovsky.watersheds.rewrite.graph.Edge;

public interface EdgeCheck
{

	public boolean isGoodEdge( Edge e );

	public static class AlwaysTrue implements EdgeCheck, Serializable
	{

		@Override
		public boolean isGoodEdge( final Edge e )
		{
			return true;
		}

	}

}