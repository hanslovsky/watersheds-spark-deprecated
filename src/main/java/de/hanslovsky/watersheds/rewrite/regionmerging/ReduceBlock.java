package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
import de.hanslovsky.watersheds.rewrite.util.DisjointSetsHashMap;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import scala.Tuple2;

public class ReduceBlock implements PairFunction< Tuple2< Long, ArrayList< RemappedData > >, Long, OriginalLabelData >
{

	@Override
	public Tuple2< Long, OriginalLabelData > call( final Tuple2< Long, ArrayList< RemappedData > > t ) throws Exception
	{
		final long key = t._1();
		final ArrayList< RemappedData > mappedDatas = t._2();

		final TDoubleArrayList allEdges = new TDoubleArrayList();
		final Edge ae = new Edge( allEdges );

		final TLongLongHashMap allCounts = new TLongLongHashMap();
		final TLongLongHashMap allOutsideNodes = new TLongLongHashMap();

		final DisjointSetsHashMap dj = new DisjointSetsHashMap();

		for ( final RemappedData md : mappedDatas )
		{
			allCounts.putAll( md.counts );
			allOutsideNodes.putAll( md.outsideNodes );
			for ( int i = 0, k = 1; i < md.merges.size(); i += 2, k += 2 )
				dj.join( dj.findRoot( md.merges.get( i ) ), dj.findRoot( md.merges.get( k ) ) );
		}

		for ( final RemappedData md : mappedDatas )
		{
			final Edge e = new Edge( md.edges );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				final long from = dj.findRoot( e.from() );
				final long to = dj.findRoot( e.to() );
				ae.add( e.weight(), e.affinity(), from, to, e.multiplicity() );
				if ( e.from() == 7085 || e.to() == 7085 )
					System.out.println( "Reducing and rooting: " + key + " " + e + " " + from + " " + to  );
			}
		}

		{
			final Edge e = new Edge( allEdges );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				if ( e.from() == 7085 || e.to() == 7085 )
					System.out.println( "Reducing: " + key + " " + e );
			}
		}

		return new Tuple2<>( key, new OriginalLabelData( allEdges, allCounts, allOutsideNodes ) );
	}

}