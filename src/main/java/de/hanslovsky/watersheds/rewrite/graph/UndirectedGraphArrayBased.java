package de.hanslovsky.watersheds.rewrite.graph;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import de.hanslovsky.watersheds.rewrite.graph.edge.Edge;
import de.hanslovsky.watersheds.rewrite.graph.edge.EdgeMerger;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

public class UndirectedGraphArrayBased implements Serializable
{

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	{
		LOG.setLevel( Level.INFO );
	}

	private final TDoubleArrayList edges;

	private final TIntIntHashMap[] nodeEdgeMap;

	private final Edge e1, e2;

	public UndirectedGraphArrayBased( final int nNodes, final EdgeMerger edgeMerger )
	{
		this( nNodes, new TDoubleArrayList(), edgeMerger );
	}

	public UndirectedGraphArrayBased( final int nNodes, final TDoubleArrayList edges, final EdgeMerger edgeMerger )
	{
		this( edges, nodeEdgeMap( edges, nNodes, edgeMerger, edgeMerger.dataSize() ), edgeMerger.dataSize() );
	}

	public UndirectedGraphArrayBased( final TDoubleArrayList edges, final TIntIntHashMap[] nodeEdgeMap, final int edgeDataSize )
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

		e.setObsolete();

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

			if ( nodeId == otherNode || this.e1.isObsolete() )
			{
				this.e1.setObsolete();
				continue;
			}

			if ( keepEdges.contains( nodeId ) )
			{
				this.e2.setIndex( keepEdges.get( nodeId ) );
				final double w1 = this.e1.weight();
				final double w2 = this.e2.weight();

				// smaller weight edge is in wrong map
				if ( w1 < w2 )
				{
					edgeMerger.merge( this.e2, this.e1 );
					keepEdges.put( nodeId, edgeId );
					this.e2.setObsolete();
				}
				else
				{
					edgeMerger.merge( this.e1, this.e2 );
					this.e1.setObsolete();
				}
			}
			else
				keepEdges.put( nodeId, edgeId );
//				this.e1.setStale();
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
			this.e1.setStale();
			this.e1.from( nodeId );
			this.e1.to( newNode );
		}

		return discardEdges;

	}

	private static TIntIntHashMap[] nodeEdgeMap( final TDoubleArrayList edges, final int nNodes, final EdgeMerger edgeMerger, final int edgeDataSize )
	{
		final TIntIntHashMap[] nodeEdgeMap = new TIntIntHashMap[ nNodes ];
		for ( int i = 0; i < nNodes; ++i )
			nodeEdgeMap[ i ] = new TIntIntHashMap();
		final Edge e1 = new Edge( edges, edgeDataSize );
		final Edge e2 = new Edge( edges, edgeDataSize );
		final int nEdges = e1.size();
		for ( int i = 0; i < nEdges; ++i )
		{
			e1.setIndex( i );
			if ( e1.isObsolete() )
				continue;
			final int from = ( int ) e1.from();
			final int to = ( int ) e1.to();

			assert from != to: e1;

			final TIntIntHashMap fromMap = nodeEdgeMap[ from ];
			final TIntIntHashMap toMap = nodeEdgeMap[ to ];

			if ( fromMap.contains( to ) || toMap.contains( from ) )
			{
				assert fromMap.get( to ) == toMap.get( from ): "Edges are inconsistent!";
				e2.setIndex( fromMap.get( to ) );
				LOG.trace( "Edge exists multiple times! " + e1 + " " + e2 + " " + fromMap + " " + toMap );
				edgeMerger.merge( e1, e2 );
				e2.setStale();
				e1.setObsolete();
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
