package de.hanslovsky.watersheds.rewrite.graph;

import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

public class UndirectedGraph
{

	private final TDoubleArrayList edges;

	private final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap;

	private final Edge e1, e2;

	public UndirectedGraph( final int edgeDataSize )
	{
		this( new TDoubleArrayList(), edgeDataSize );
	}

	public UndirectedGraph( final TDoubleArrayList edges, final int edgeDataSize )
	{
		this( edges, nodeEdgeMap( edges, edgeDataSize ), edgeDataSize );
	}

	private UndirectedGraph( final TDoubleArrayList edges, final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap, final int edgeDataSize )
	{
		this.edges = edges;
		this.nodeEdgeMap = nodeEdgeMap;
		this.e1 = new Edge( edges, edgeDataSize );
		this.e2 = new Edge( edges, edgeDataSize );
	}

	public TDoubleArrayList edges()
	{
		return edges;
	}

	public TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap()
	{
		return nodeEdgeMap;
	}

	public TLongIntHashMap contract(
			final Edge e,
			final long newNode,
			final EdgeMerger edgeMerger )
	{
		assert newNode == e.from() || newNode == e.to(): "New node index must be either from or to index";

		final long from = e.from();
		final long to = e.to();

		final long otherNode = from == newNode ? to : from;

		final TLongIntHashMap keepEdges = nodeEdgeMap.get( newNode );
		final TLongIntHashMap discardEdges = nodeEdgeMap.get( otherNode );
		this.e1.setIndex( keepEdges.remove( otherNode ) );
		this.e1.weight( -1.0d );
		discardEdges.remove( newNode );

		for ( final TLongIntIterator discardIt = discardEdges.iterator(); discardIt.hasNext(); )
		{
			discardIt.advance();
			final long nodeId = discardIt.key();
			final int edgeId = discardIt.value();

			this.e1.setIndex( edgeId );

			if ( keepEdges.contains( nodeId ) )
			{
				this.e2.setIndex( keepEdges.get( nodeId ) );
				edgeMerger.merge( this.e1, this.e2 );
				this.e1.weight( -1.0d );

			}
			else
				keepEdges.put( nodeId, edgeId );


		}

		for ( final TLongIntIterator keepIt = keepEdges.iterator(); keepIt.hasNext(); )
		{
			keepIt.advance();
			final long nodeId = keepIt.key();
			final int edgeId = keepIt.value();

			final TLongIntHashMap otherMap = nodeEdgeMap.get( nodeId );
			otherMap.remove( from );
			otherMap.remove( to );
			otherMap.put( newNode, edgeId );
			this.e1.setIndex( edgeId );
			this.e1.weight( Double.NaN );
		}

		return discardEdges;

	}

	private static TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap( final TDoubleArrayList edges, final int edgeDataSize )
	{
		final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap = new TLongObjectHashMap<>();
		final Edge e1 = new Edge( edges, edgeDataSize );
		final Edge e2 = new Edge( edges, edgeDataSize );
		final int nEdges = e1.size();
		for ( int i = 0; i < nEdges; ++i )
		{
			e1.setIndex( i );
			final long from = e1.from();
			final long to = e1.to();
			if ( !nodeEdgeMap.contains( from ) )
				nodeEdgeMap.put( from, new TLongIntHashMap() );
			if ( !nodeEdgeMap.contains( to ) )
				nodeEdgeMap.put( to, new TLongIntHashMap() );

			final TLongIntHashMap fromMap = nodeEdgeMap.get( from );
			final TLongIntHashMap toMap = nodeEdgeMap.get( to );

			if ( fromMap.contains( to ) || toMap.contains( from ) )
			{
				assert fromMap.get( to ) == toMap.get( from ): "Edges are inconsistent!";
				e2.setIndex( fromMap.get( to ) );
				if ( e1.weight() < e2.weight() )
				{
					fromMap.put( to, i );
					toMap.put( from, i );
					e2.weight( -1.0d );
				}
				else
					e1.weight( -1.0d );
			}
			else
			{
				fromMap.put( to, i );
				toMap.put( from, i );
			}

		}

		return nodeEdgeMap;
	}

}
