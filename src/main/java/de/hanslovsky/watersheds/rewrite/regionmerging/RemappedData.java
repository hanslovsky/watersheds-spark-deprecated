package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.io.Serializable;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class RemappedData implements Serializable
{

	public final TDoubleArrayList edges;

	public final TLongLongHashMap counts;

	public final TLongLongHashMap outsideNodes;

	public final TLongObjectHashMap< TLongHashSet > borderNodes;

	public final TLongLongHashMap borderNodeAssignments;

	public RemappedData( final TDoubleArrayList edges, final TLongLongHashMap counts, final TLongLongHashMap outsideNodes, final TLongObjectHashMap< TLongHashSet > borderNodes, final TLongLongHashMap borderNodeAssignments )
	{
		super();
		this.edges = edges;
		this.counts = counts;
		this.outsideNodes = outsideNodes;
		this.borderNodes = borderNodes;
		this.borderNodeAssignments = borderNodeAssignments;
	}

}