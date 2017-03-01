package de.hanslovsky.watersheds.rewrite.preparation;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.graph.EdgeMerger;
import de.hanslovsky.watersheds.rewrite.preparation.PrepareRegionMergingCutBlocks.BlockDivision;
import de.hanslovsky.watersheds.rewrite.preparation.PrepareRegionMergingCutBlocks.GetExternalEdges;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

public class MapNodesAndSplitBlocks
implements Function< GetExternalEdges.BlockOutput, TLongObjectHashMap< BlockDivision > >
{

	public static final Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	{
		LOG.setLevel( Level.DEBUG );
	}

	private final Broadcast< TLongLongHashMap > nodeBlockMapping;

	private final EdgeMerger edgeMerger;

	public MapNodesAndSplitBlocks( final Broadcast< TLongLongHashMap > nodeBlockMapping, final EdgeMerger edgeMerger )
	{
		super();
		this.nodeBlockMapping = nodeBlockMapping;
		this.edgeMerger = edgeMerger;
	}

	@Override
	public TLongObjectHashMap< BlockDivision > call( final GetExternalEdges.BlockOutput o ) throws Exception
	{

		final TLongLongHashMap nodeBlockMapping = this.nodeBlockMapping.getValue();

		final TLongObjectHashMap< BlockDivision > regionMergingInputs = new TLongObjectHashMap<>();

		LOG.trace( "Initial block contains " + o.blockIds.length + " sub-blocks: " + Arrays.toString( o.blockIds ) );

		for ( final long id : o.blockIds ) {
			final BlockDivision in = new BlockDivision( new TLongLongHashMap(), new TLongLongHashMap(), new TLongObjectHashMap<>(), new TDoubleArrayList(), new TLongObjectHashMap<>() );
			regionMergingInputs.put( id, in );
		}

		final Edge e = new Edge( o.edges );
		final int nEdges = e.size();

		// add intra block edges
		for ( int i = 0; i < o.numberOfInternalEdges; ++i )
		{
			e.setIndex( i );
			final double w = e.weight();
			final double a = e.affinity();
			final long from = e.from();
			final long to = e.to();
			final long m = e.multiplicity();
			final long r1 = o.nodeBlockAssignment.get( from );
			final long r2 = o.nodeBlockAssignment.get( to );
			if ( r1 == r2 )
			{
				final BlockDivision in = regionMergingInputs.get( r1 );
				if ( !in.counts.contains( from ) )
					in.counts.put( from, o.counts.get( from ) );
				if ( !in.counts.contains( to ) )
					in.counts.put( to, o.counts.get( to ) );

				if ( !in.nodeEdgeMap.contains( from ) )
					in.nodeEdgeMap.put( from, new TLongIntHashMap() );
				if ( !in.nodeEdgeMap.contains( to ) )
					in.nodeEdgeMap.put( to, new TLongIntHashMap() );

				final int index = in.e1.add( w, a, from, to, m );

				in.nodeEdgeMap.get( from ).put( to, index );
				in.nodeEdgeMap.get( to ).put( from, index );


			}
			else
			{
				final BlockDivision in1 = regionMergingInputs.get( r1 );
				if ( !in1.counts.contains( from ) )
					in1.counts.put( from, o.counts.get( from ) );
				if ( !in1.counts.contains( to ) )
					in1.counts.put( to, o.counts.get( to ) );
				if ( !in1.nodeEdgeMap.contains( from ) )
					in1.nodeEdgeMap.put( from, new TLongIntHashMap() );
				if ( !in1.nodeEdgeMap.contains( to ) )
					in1.nodeEdgeMap.put( to, new TLongIntHashMap() );

				final BlockDivision in2 = regionMergingInputs.get( r2 );
				if ( !in2.counts.contains( from ) )
					in2.counts.put( from, o.counts.get( from ) );
				if ( !in2.counts.contains( to ) )
					in2.counts.put( to, o.counts.get( to ) );
				if ( !in2.nodeEdgeMap.contains( from ) )
					in2.nodeEdgeMap.put( from, new TLongIntHashMap() );
				if ( !in2.nodeEdgeMap.contains( to ) )
					in2.nodeEdgeMap.put( to, new TLongIntHashMap() );

				final int index1 = in1.e1.add( w, a, from, to, m );
				in1.nodeEdgeMap.get( from ).put( to, index1 );
				in1.nodeEdgeMap.get( to ).put( from, index1 );

				final int index2 = in2.e1.add( w, a, from, to, m );
				in2.nodeEdgeMap.get( from ).put( to, index2 );
				in2.nodeEdgeMap.get( to ).put( from, index2 );

				in1.outsideNodes.put( to, r2 );
				in2.outsideNodes.put( from, r1 ); // was in1.,outsideNodes
			}
		}

		// add edges that reach beyond block border
		for ( int i = o.numberOfInternalEdges; i < nEdges; ++i ) {
			e.setIndex( i );
			final double w = e.weight();
			final double a = e.affinity();
			final long from = e.from(); // inner
			final long to = e.to(); // outer
			final long m = e.multiplicity();

			final long n1, n2;
			if ( o.nodeBlockAssignment.contains( from ) )
			{
				n1 = from;
				n2 = to;
			} else
			{
				n1 = to;
				n2 = from;
			}

			final long r1 = o.nodeBlockAssignment.get( n1 );
			final long r2 = nodeBlockMapping.get( n2 );
			final BlockDivision in = regionMergingInputs.get( r1 );

			if ( !in.counts.contains( n1 ) )
				in.counts.put( n1, o.counts.get( n1 ) );

			if ( !in.counts.contains( n2 ) )
				in.counts.put( n2, o.counts.get( n2 ) );

			if ( !in.nodeEdgeMap.contains( n1 ) )
				in.nodeEdgeMap.put( n1, new TLongIntHashMap() );
			if ( !in.nodeEdgeMap.contains( n2 ) )
				in.nodeEdgeMap.put( n2, new TLongIntHashMap() );

			final int index = in.e1.add( w, a, from, to, m );

			in.nodeEdgeMap.get( from ).put( to, index );
			in.nodeEdgeMap.get( to ).put( from, index );

			if ( !in.outsideNodes.contains( n2 ) )
				in.outsideNodes.put( n2, r2 );

		}


		return regionMergingInputs;
	}

}