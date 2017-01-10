package de.hanslovsky.watersheds.rewrite.mergebloc;

import de.hanslovsky.watersheds.rewrite.graph.UndirectedGraphArrayBased;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class MergeBlocIn
{

	public final UndirectedGraphArrayBased g;

	public final long[] counts;

	public final TIntLongHashMap outsideNodes;

	public final TIntObjectHashMap< TLongHashSet > borderNodes;

	public MergeBlocIn( final UndirectedGraphArrayBased g, final long[] counts, final TIntLongHashMap outsideNodes, final TIntObjectHashMap< TLongHashSet > borderNodes )
	{
		super();
		this.g = g;
		this.counts = counts;
		this.outsideNodes = outsideNodes;
		this.borderNodes = borderNodes;
	}

}