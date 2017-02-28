package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.lang.invoke.MethodHandles;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;

import de.hanslovsky.watersheds.DisjointSetsHashMap;
import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocOut;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import scala.Tuple2;

public class RemapToOriginalIndices implements Function< Tuple2< Long, MergeBlocOut >, Tuple2< Long, RemappedData > >
{

	public static Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	{
		LOG.setLevel( Level.TRACE );
	}

	@Override
	public Tuple2< Long, RemappedData > call( final Tuple2< Long, MergeBlocOut > t ) throws Exception
	{
		final MergeBlocOut out = t._2();
		final long[] map = out.indexNodeMapping;

		final TDoubleArrayList edges = new TDoubleArrayList( out.edges );

		// map back edges
		Util.remapEdges( new Edge( edges ), out, map );

		// map back counts
		final TLongLongHashMap countsInBlock = Util.remapCounts( out, map );

		// map back outsideNodes
		final TLongLongHashMap outsideNodes = Util.remapOutsideNodes( out, map );

		// map back borderNodeMappings
		final TLongLongHashMap borderNodeMappings = new TLongLongHashMap();
		final DisjointSetsHashMap borderNodeUnionFind = new DisjointSetsHashMap( t._2().borderNodeMappings, new TLongLongHashMap(), 0 );
		for ( final long key : t._2().borderNodeMappings.keys() )
			borderNodeMappings.put( map[ ( int ) key ], map[ ( int ) borderNodeUnionFind.findRoot( key ) ] );

		final TLongArrayList merges = new TLongArrayList();
		for ( int i = 0; i < out.merges.size(); i += 4 )
		{
			merges.add( map[ ( int ) out.merges.get( i ) ] );
			merges.add( map[ ( int ) out.merges.get( i + 1 ) ] );
			merges.add( map[ ( int ) out.merges.get( i + 2 ) ] );
			merges.add( out.merges.get( i + 3 ) );
			// do i need to add merges[ i + 2]?
		}

		if ( LOG.getLevel().isGreaterOrEqual( Level.TRACE ) )
		{
			final Edge e = new Edge( edges );
			final StringBuilder sb = new StringBuilder( "Logging re-mapped edges: " );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				sb.append( "\n" ).append( e.toString() );
			}
			sb.append( "\n" ).append( "Logging re-mapped counts:\n" ).append( countsInBlock.toString() );
			sb.append( "\n" ).append( "Logging re-mapped outside Nodes:\n" ).append( outsideNodes.toString() );
			sb.append( "\n" ).append( "Logging borderNodeMappings:\n" ).append( borderNodeMappings.toString() );
			sb.append( "\n" ).append( "Logging re-mapped merges:\n" ).append( merges );
			LOG.trace( sb.toString() );

		}

		return new Tuple2<>( t._1(), new RemappedData( edges, countsInBlock, outsideNodes, merges, borderNodeMappings ) );
	}

}