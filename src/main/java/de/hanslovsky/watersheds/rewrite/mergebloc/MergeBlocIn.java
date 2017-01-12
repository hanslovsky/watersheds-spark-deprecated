package de.hanslovsky.watersheds.rewrite.mergebloc;

import java.io.Serializable;

import de.hanslovsky.watersheds.rewrite.graph.UndirectedGraphArrayBased;
import gnu.trove.map.hash.TIntLongHashMap;

public class MergeBlocIn implements Serializable
{

	public final UndirectedGraphArrayBased g;

	public final long[] counts;

	public final TIntLongHashMap outsideNodes;

	public final long[] indexNodeMapping;

	public MergeBlocIn(
			final UndirectedGraphArrayBased g,
			final long[] counts,
			final TIntLongHashMap outsideNodes,
			final long[] indexNodeMapping )
	{
		super();
		this.g = g;
		this.counts = counts;
		this.outsideNodes = outsideNodes;
		this.indexNodeMapping = indexNodeMapping;
	}

}