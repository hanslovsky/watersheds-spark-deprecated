package de.hanslovsky.watersheds.graph;

public interface Function
{

	double weight( double affinity, long count1, long count2 );
}