package de.hanslovsky.watersheds.rewrite.regionmerging;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongLongHashMap;

public class OriginalLabelData
{

	public final TDoubleArrayList edges;

	public final TLongLongHashMap counts;

	public final TLongLongHashMap outsideNodes;

	public OriginalLabelData( final TDoubleArrayList edges, final TLongLongHashMap counts, final TLongLongHashMap outsideNodes )
	{
		super();
		this.edges = edges;
		this.counts = counts;
		this.outsideNodes = outsideNodes;
	}

}