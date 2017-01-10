package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.io.Serializable;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class RegionMergingInput implements Serializable
{

	public final int nNodes;

	public final TLongIntHashMap nodeIndexMapping;

	public final TLongLongHashMap counts;

	public final TLongLongHashMap outsideNodes;

	public final TDoubleArrayList edges;

	public final TLongObjectHashMap< TLongHashSet > borderNodes;

	public RegionMergingInput( final int nNodes, final TLongIntHashMap nodeIndexMapping, final TLongLongHashMap counts, final TLongLongHashMap outsideNodes, final TDoubleArrayList edges, final TLongObjectHashMap< TLongHashSet > borderNodes )
	{
		super();
		this.nNodes = nNodes;
		this.nodeIndexMapping = nodeIndexMapping;
		this.counts = counts;
		this.outsideNodes = outsideNodes;
		this.edges = edges;
		this.borderNodes = borderNodes;
	}


}