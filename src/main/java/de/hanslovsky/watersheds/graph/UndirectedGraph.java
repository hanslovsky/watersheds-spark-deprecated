package de.hanslovsky.watersheds.graph;

import java.io.Serializable;

import de.hanslovsky.watersheds.DisjointSetsHashMap;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

public class UndirectedGraph implements Serializable
{

	private final TDoubleArrayList edges;

	private final Edge e1, e2, e3;

	private final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap;

	private final EdgeMerger edgeMerger;

	public UndirectedGraph( final TDoubleArrayList edges, final EdgeMerger edgeMerger )
	{
		this( edges, generateNodeEdgeMap( edges ), edgeMerger );
	}

	public UndirectedGraph( final TDoubleArrayList edges, final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap, final EdgeMerger edgeMerger )
	{
		super();
		this.edges = edges;
		this.e1 = new Edge( this.edges );
		this.e2 = new Edge( this.edges );
		this.e3 = new Edge( this.edges );
		this.nodeEdgeMap = nodeEdgeMap;
		this.edgeMerger = edgeMerger;
	}

	public TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap()
	{
		return this.nodeEdgeMap;
	}

	public TDoubleArrayList edges()
	{
		return this.edges;
	}

	public EdgeMerger edgeMerger()
	{
		return this.edgeMerger;
	}

	public boolean addNode( final long id )
	{
		if ( nodeEdgeMap.contains( id ) )
			return false;

		nodeEdgeMap.put( id, new TLongIntHashMap() );

		return true;
	}

	public int addEdge( final double weight, final double affinity, final long from, final long to, final long multiplicity )
	{

		if ( weight < 0.0 )
			return -1;

		if ( !nodeEdgeMap.contains( from ) )
			nodeEdgeMap.put( from, new TLongIntHashMap() );
		if ( !nodeEdgeMap.contains( to ) )
			nodeEdgeMap.put( to, new TLongIntHashMap() );
//		if ( !nodeEdgeMap.contains( from ) || !nodeEdgeMap.contains( to ) )
//			return -1;

		final int newEdge = e1.add( weight, affinity, from, to, multiplicity );
		final TLongIntHashMap n1 = nodeEdgeMap.get( from );
		final TLongIntHashMap n2 = nodeEdgeMap.get( to );

		if ( n1.contains( to ) && n2.contains( from ) )
		{
			e2.setIndex( n1.get( to ) );
			if ( weight < e2.weight() )
			{
				n1.put( to, newEdge );
				n2.put( from, newEdge );
			}
		}
		else
		{
			n1.put( to, newEdge );
			n2.put( from, newEdge );
		}
		return newEdge;
	}

	public boolean removeNode( final long id )
	{
		if ( !nodeEdgeMap.contains( id ) )
			return false;

		for ( final TLongIntIterator it = nodeEdgeMap.get( id ).iterator(); it.hasNext(); )
		{
			it.advance();
			nodeEdgeMap.get( it.key() ).remove( id );
			e2.setIndex( it.value() );
			e2.weight( -1.0d );
		}

		return true;
	}

	public TLongIntHashMap contract(
			final int edge,
			final long r1,
			final long r2,
			final long newNode,
			final TLongLongHashMap counts,
			final DisjointSetsHashMap dj,
			final Function f )
	{
//		final TLongIntHashMap edges = new TLongIntHashMap();

		e1.setIndex( edge );
		final long from = e1.from();
		final long to = e1.to();
		if ( !nodeEdgeMap.contains( from ) || !nodeEdgeMap.contains( to ) )
			return null;
		e1.weight( -1.0 );

//		updateEdges( from, newNode, edge, to, edges, this );
//		updateEdges( to, newNode, edge, from, edges, this );

		final long otherNode = newNode == from ? to : from;
		final TLongIntHashMap edges = nodeEdgeMap.get( newNode );
		final TLongIntHashMap otherEdges = nodeEdgeMap.get( otherNode );

		edges.remove( otherNode );

		for ( final TLongIntIterator v = otherEdges.iterator(); v.hasNext(); )
		{
			v.advance();
			final long k = v.key();
			final int id = v.value();
			e1.setIndex( id );

			if ( k == newNode )
				continue;// e1.weight( -1.0 );

			// if edge is present in new node (duplicate edges), do a merge! Set
			// old edge to invalid!
			if ( edges.contains( k ) )
			{
				e2.setIndex( edges.get( k ) );
				this.edgeMerger.merge( e1, e2 );
				e1.weight( -1.0 );
				e2.from( dj.findRoot( e2.from() ) );
				e2.to( dj.findRoot( e2.to() ) );
				e2.weight( f.weight( e2.affinity(), counts.get( e2.from() ), counts.get( e2.to() ) ) );
			}
			// else: add edge to existing set of edges for newNode
			else
			{
				edges.put( k, id );
				e1.from( dj.findRoot( e1.to() ) );
				e1.to( dj.findRoot( e1.to() ) );
				e1.weight( f.weight( e1.affinity(), counts.get( e2.from() ), counts.get( e2.to() ) ) );
			}

			nodeEdgeMap.get( k ).remove( otherNode );
		}

		// update edges of neighbors
		for ( final TLongIntIterator it = edges.iterator(); it.hasNext(); )
		{
			it.advance();
			final TLongIntHashMap nem = nodeEdgeMap.get( it.key() );
			nem.remove( from );
			nem.remove( to );
			nem.put( newNode, it.value() );
			e1.setIndex( it.value() );
			e1.from( newNode );
			e1.to( it.key() );
		}

//		nodeEdgeMap.put( newNode, edges );
		nodeEdgeMap.remove( otherNode );

		return otherEdges;

	}

	private static void updateEdges(
			final long oldIndex,
			final long newIndex,
			final int edge,
			final long ignoreIndex,
			final TLongIntHashMap newEdges,
			final UndirectedGraph g )
	{
//		System.out.println( String.format( "old=%d new=%d ignore=%d", oldIndex, newIndex, ignoreIndex ) );
//		System.out.println( "Removing map at " + oldIndex );
		final TLongIntHashMap edges = g.nodeEdgeMap.remove( oldIndex );

		for ( final TLongIntIterator it = edges.iterator(); it.hasNext(); )
		{
			it.advance();
			final long otherIndex = it.key();
			final int edgeIndex = it.value();

			g.e2.setIndex( edgeIndex );

			if ( otherIndex == ignoreIndex )
				continue;

			if ( newEdges.contains( otherIndex ) )
			{
				g.e3.setIndex( newEdges.get( otherIndex ) );
				g.edgeMerger.merge( g.e2, g.e3 );
			}
			else {
				final int newEdgeIndex = g.e3.add(
						g.e2.weight(),
						g.e2.affinity(),
						newIndex,
						otherIndex,
						g.e2.multiplicity() );
				newEdges.put( otherIndex, newEdgeIndex );
				g.nodeEdgeMap.get( otherIndex ).put( newIndex, newEdgeIndex );
			}

			// TODO is second line also part of condition?
			if ( oldIndex != newIndex )
				g.nodeEdgeMap.get( otherIndex ).remove( oldIndex );

			g.e2.weight( -1.0 );

		}

	}

	public static TLongObjectHashMap< TLongIntHashMap > generateNodeEdgeMap( final TDoubleArrayList edges )
	{
		final TLongObjectHashMap< TLongIntHashMap > nodeEdgeMap = new TLongObjectHashMap<>();
		final Edge e = new Edge( edges );
		for ( int i = 0; i < e.size(); ++i )
		{
			e.setIndex( i );
			final long from = e.from();
			final long to = e.to();
			if ( !nodeEdgeMap.contains( from ) )
				nodeEdgeMap.put( from, new TLongIntHashMap() );
			if ( !nodeEdgeMap.contains( to ) )
				nodeEdgeMap.put( to, new TLongIntHashMap() );
			final TLongIntHashMap f = nodeEdgeMap.get( from );
			final TLongIntHashMap t = nodeEdgeMap.get( to );
			if ( f.contains( to ) && t.contains( from ) )
				e.weight( -1.0d );
			else
			{
				f.put( to, i );
				t.put( from, i );
			}

		}
		return nodeEdgeMap;
	}

}

