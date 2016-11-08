package de.hanslovsky.watersheds.graph;

import java.io.Serializable;

import gnu.trove.impl.Constants;
import gnu.trove.map.hash.TLongLongHashMap;

public class HashSetsDisjointSets implements Serializable
{

	public static final long DEFAULT_NO_ENTRY_VALUE = -1;
	/**
	 *
	 */
	private static final long serialVersionUID = -955128861938966488L;

	private final TLongLongHashMap parents;

	private final TLongLongHashMap ranks;

	private int nSets;

	private final long getNoEntryValue;

	public HashSetsDisjointSets() {
		this(
				new TLongLongHashMap(
						Constants.DEFAULT_CAPACITY,
						Constants.DEFAULT_LOAD_FACTOR,
						DEFAULT_NO_ENTRY_VALUE,
						DEFAULT_NO_ENTRY_VALUE ),
				new TLongLongHashMap(),
				1 );
	}

	public HashSetsDisjointSets( final TLongLongHashMap parents, final TLongLongHashMap ranks, final int nSets )
	{
		this.parents = parents;
		this.ranks = ranks;
		this.nSets = nSets;
		this.getNoEntryValue = parents.getNoEntryValue();
	}

	public long findRoot( final long id )
	{

		long startIndex1 = id;
		long startIndex2 = id;
		long tmp = id;

		while ( true )
		{
			long p = parents.get( startIndex1 );
			if ( p == DEFAULT_NO_ENTRY_VALUE )
			{
				parents.put( startIndex1, startIndex1 );
				ranks.put( startIndex1, 0 );
				p = startIndex1;
				++nSets;
			}
			if ( p == startIndex1 )
				break;
			else
				startIndex1 = p;
		}

		// label all positions on the way to root as parent
		while ( startIndex2 != startIndex1 )
		{
			tmp = parents.get( startIndex2 );
			parents.put( startIndex2, startIndex1 );
			startIndex2 = tmp;
		}

		return startIndex1;

	}

	public long join( final long id1, final long id2 )
	{

		if ( id1 == id2 )
			return id1;

		--nSets;

		final long r1 = ranks.get( id1 );
		final long r2 = ranks.get( id2 );

		if ( r1 < r2 )
		{
			parents.put( id1, id2 );
			return id2;
		}

		else
		{
			parents.put( id2, id1 );
			if ( r1 == r2 )
				ranks.put( id1, r1 + 1 );
			return id1;
		}

	}

	public int size()
	{
		return parents.size();
	}

	public int setCount()
	{
		return nSets;
	}

//	@Override
//	public DisjointSets clone()
//	{
//		return new DisjointSets( parents. );
//	}

}
