package de.hanslovsky.watersheds.rewrite.mergebloc;

import de.hanslovsky.watersheds.rewrite.graph.UndirectedGraphArrayBased;
import de.hanslovsky.watersheds.rewrite.util.ChangeablePriorityQueue;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;

public class MergeBlocOut
{

	public final UndirectedGraphArrayBased g;

	public final long[] counts;

	public final TIntLongHashMap outsideNodes;

	public final ChangeablePriorityQueue queue;

	public final DisjointSets dj;

	public final TIntObjectHashMap< TLongHashSet > borderNodes;

	public final boolean hasChanged;

	public MergeBlocOut( final UndirectedGraphArrayBased g, final long[] counts, final TIntLongHashMap outsideNodes, final ChangeablePriorityQueue queue, final DisjointSets dj, final TIntObjectHashMap< TLongHashSet > borderNodes, final boolean hasChanged )
	{
		super();
		this.g = g;
		this.counts = counts;
		this.outsideNodes = outsideNodes;
		this.queue = queue;
		this.dj = dj;
		this.borderNodes = borderNodes;
		this.hasChanged = hasChanged;
	}

}