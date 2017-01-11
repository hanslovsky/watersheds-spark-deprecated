package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.io.Serializable;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;

public class RemappedData implements Serializable
{

	public final TDoubleArrayList edges;

	public final TLongLongHashMap counts;

	public final TLongLongHashMap outsideNodes;

	public final TLongArrayList merges;

	public RemappedData( final TDoubleArrayList edges, final TLongLongHashMap counts, final TLongLongHashMap outsideNodes, final TLongArrayList merges )
	{
		super();
		this.edges = edges;
		this.counts = counts;
		this.outsideNodes = outsideNodes;
		this.merges = merges;
	}

}