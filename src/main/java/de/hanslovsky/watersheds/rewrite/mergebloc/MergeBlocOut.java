package de.hanslovsky.watersheds.rewrite.mergebloc;

import de.hanslovsky.watersheds.rewrite.ChangeablePriorityQueue;
import de.hanslovsky.watersheds.rewrite.UndirectedGraphArrayBased;
import gnu.trove.map.hash.TLongLongHashMap;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;

public class MergeBlocOut
{

	public final UndirectedGraphArrayBased g;

	public final long[] counts;

	public final TLongLongHashMap outsideNodes;

	public final ChangeablePriorityQueue queue;

	public final DisjointSets dj;

	public MergeBlocOut( final UndirectedGraphArrayBased g, final long[] counts, final TLongLongHashMap outsideNodes, final ChangeablePriorityQueue queue, final DisjointSets dj )
	{
		super();
		this.g = g;
		this.counts = counts;
		this.outsideNodes = outsideNodes;
		this.queue = queue;
		this.dj = dj;
	}

}