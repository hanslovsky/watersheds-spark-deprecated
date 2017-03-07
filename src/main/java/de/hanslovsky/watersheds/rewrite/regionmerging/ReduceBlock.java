package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.util.ArrayList;

import org.apache.spark.api.java.function.Function;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.util.DisjointSetsHashMap;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongLongHashMap;

public class ReduceBlock implements Function< ArrayList< RemappedData >, OriginalLabelData >
{

	private final int edgeDataSize;

	public ReduceBlock( final int edgeDataSize )
	{
		super();
		this.edgeDataSize = edgeDataSize;
	}

	@Override
	public OriginalLabelData call( final ArrayList< RemappedData > mappedDatas ) throws Exception
	{
		final TDoubleArrayList allEdges = new TDoubleArrayList();
		final Edge ae = new Edge( allEdges, edgeDataSize );

		final TLongLongHashMap allCounts = new TLongLongHashMap();
		final TLongLongHashMap allOutsideNodes = new TLongLongHashMap();
		final TLongLongHashMap parents = new TLongLongHashMap();

		for ( final RemappedData md : mappedDatas )
		{
			allCounts.putAll( md.counts );
			allOutsideNodes.putAll( md.outsideNodes );
			for ( int i = 0, k = 1, n = 2; i < md.merges.size(); i += 4, k += 4, n += 4 )
			{
				parents.put( md.merges.get( i ), md.merges.get( n ) );
				parents.put( md.merges.get( k ), md.merges.get( n ) );
			}
//				dj.join( dj.findRoot( md.merges.get( i ) ), dj.findRoot( md.merges.get( k ) ) );
		}

		final DisjointSetsHashMap dj = new DisjointSetsHashMap( parents, new TLongLongHashMap(), 0 );

		for ( final RemappedData md : mappedDatas )
		{
			final Edge e = new Edge( md.edges, edgeDataSize );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				final long from = dj.findRoot( e.from() );
				final long to = dj.findRoot( e.to() );
				ae.add( e );
				ae.from( from );
				ae.to( to );
			}
		}

		return new OriginalLabelData( allEdges, allCounts, allOutsideNodes );
	}

}