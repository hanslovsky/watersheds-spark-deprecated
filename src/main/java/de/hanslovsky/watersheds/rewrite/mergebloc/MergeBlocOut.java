package de.hanslovsky.watersheds.rewrite.mergebloc;

import java.io.Serializable;

import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;

public class MergeBlocOut implements Serializable
{

	public final long[] counts;

	public final TIntLongHashMap outsideNodes;

	public final DisjointSets dj;

	public final TIntObjectHashMap< TLongHashSet > borderNodes;

	public final boolean hasChanged;

	public final TDoubleArrayList edges;

	public final TLongArrayList merges;

	public final long[] indexNodeMapping;

	public MergeBlocOut(
			final long[] counts,
			final TIntLongHashMap outsideNodes,
			final DisjointSets dj,
			final TIntObjectHashMap< TLongHashSet > borderNodes,
			final boolean hasChanged,
			final TDoubleArrayList edges,
			final TLongArrayList merges,
			final long[] indexNodeMapping )
	{
		super();
		this.counts = counts;
		this.outsideNodes = outsideNodes;
		this.dj = dj;
		this.borderNodes = borderNodes;
		this.hasChanged = hasChanged;
		this.edges = edges;
		this.merges = merges;
		this.indexNodeMapping = indexNodeMapping;
	}

}