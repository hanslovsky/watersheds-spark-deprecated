package de.hanslovsky.watersheds.rewrite.graph;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

public class UndirectedGraphArrayBased
{

	private final TDoubleArrayList edges;

	private final TIntIntHashMap[] nodeEdgeMap;

	private final Edge e1, e2;

	public UndirectedGraphArrayBased( final int nNodes )
	{
		this( nNodes, new TDoubleArrayList() );
	}

	public UndirectedGraphArrayBased( final int nNodes, final TDoubleArrayList edges )
	{
		this( edges, nodeEdgeMap( edges, nNodes ) );
	}

	private UndirectedGraphArrayBased( final TDoubleArrayList edges, final TIntIntHashMap[] nodeEdgeMap )
	{
		this.edges = edges;
		this.nodeEdgeMap = nodeEdgeMap;
		this.e1 = new Edge( edges );
		this.e2 = new Edge( edges );
	}

	public TDoubleArrayList edges()
	{
		return edges;
	}

	public TIntIntHashMap[] nodeEdgeMap()
	{
		return nodeEdgeMap;
	}

	public int nNodes()
	{
		return nodeEdgeMap.length;
	}

	public TIntIntHashMap contract(
			final Edge e,
			final int newNode,
			final EdgeMerger edgeMerger )
	{
		assert newNode == e.from() || newNode == e.to(): "New node index must be either from or to index";

		final int from = ( int ) e.from();
		final int to = ( int ) e.to();

		final int otherNode = from == newNode ? to : from;

		e.weight( -1.0d );

		final TIntIntHashMap keepEdges = nodeEdgeMap[ newNode ];
		final TIntIntHashMap discardEdges = nodeEdgeMap[ otherNode ];

		keepEdges.remove( otherNode );
		discardEdges.remove( newNode );

		for ( final TIntIntIterator discardIt = discardEdges.iterator(); discardIt.hasNext(); )
		{
			discardIt.advance();
			final int nodeId = discardIt.key();
			final int edgeId = discardIt.value();

			this.e1.setIndex( edgeId );

			if ( nodeId == otherNode || this.e1.weight() < 0.0d )
				continue;

			if ( keepEdges.contains( nodeId ) )
			{
				this.e2.setIndex( keepEdges.get( nodeId ) );
				edgeMerger.merge( this.e1, this.e2 );
				this.e1.weight( -1.0d );

			}
			else
			{
				keepEdges.put( nodeId, edgeId );
				this.e1.weight( Double.NaN );
			}
		}


		for ( final TIntIntIterator keepIt = keepEdges.iterator(); keepIt.hasNext(); )
		{
			keepIt.advance();
			final int nodeId = keepIt.key();
			final int edgeId = keepIt.value();

			final TIntIntHashMap otherMap = nodeEdgeMap[ nodeId ];
			otherMap.remove( from );
			otherMap.remove( to );
			otherMap.put( newNode, edgeId );
			this.e1.setIndex( edgeId );
			this.e1.weight( Double.NaN );
			this.e1.from( nodeId );
			this.e1.to( newNode );
		}

		return discardEdges;

	}

	private static TIntIntHashMap[] nodeEdgeMap( final TDoubleArrayList edges, final int nNodes )
	{
		final TIntIntHashMap[] nodeEdgeMap = new TIntIntHashMap[ nNodes ];
		for ( int i = 0; i < nNodes; ++i )
			nodeEdgeMap[ i ] = new TIntIntHashMap();
		final Edge e1 = new Edge( edges );
		final Edge e2 = new Edge( edges );
		final int nEdges = e1.size();
		for ( int i = 0; i < nEdges; ++i )
		{
			e1.setIndex( i );
			final int from = ( int ) e1.from();
			final int to = ( int ) e1.to();

			final TIntIntHashMap fromMap = nodeEdgeMap[ from ];
			final TIntIntHashMap toMap = nodeEdgeMap[ to ];

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
