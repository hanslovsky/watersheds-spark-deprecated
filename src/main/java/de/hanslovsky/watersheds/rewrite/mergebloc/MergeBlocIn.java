package de.hanslovsky.watersheds.rewrite.mergebloc;

import de.hanslovsky.watersheds.rewrite.UndirectedGraphArrayBased;
import gnu.trove.map.hash.TLongLongHashMap;

public class MergeBlocIn
{

	public final UndirectedGraphArrayBased g;

	public final long[] counts;

	public final TLongLongHashMap outsideNodes;

	public MergeBlocIn( final UndirectedGraphArrayBased g, final long[] counts, final TLongLongHashMap outsideNodes )
	{
		super();
		this.g = g;
		this.counts = counts;
		this.outsideNodes = outsideNodes;
	}

}