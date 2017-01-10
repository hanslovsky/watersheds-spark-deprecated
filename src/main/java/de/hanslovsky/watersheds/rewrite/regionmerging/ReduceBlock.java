package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.util.ArrayList;

import org.apache.spark.api.java.function.PairFunction;

import de.hanslovsky.watersheds.rewrite.Edge;
import de.hanslovsky.watersheds.rewrite.regionmerging.RegionMergingArrayBased.OriginalLabelData;
import de.hanslovsky.watersheds.rewrite.regionmerging.RegionMergingArrayBased.RemappedData;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
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
		final TLongLongHashMap allBorderNodesAssignments = new TLongLongHashMap();
		final TLongLongHashMap allOutsideNodes = new TLongLongHashMap();
		final TLongObjectHashMap< TLongHashSet > allBorderNodes = new TLongObjectHashMap<>();

		for ( final RemappedData md : mappedDatas )
		{
			allBorderNodesAssignments.putAll( md.borderNodeAssignments );
			allCounts.putAll( allCounts );
			allOutsideNodes.putAll( md.outsideNodes );
			allBorderNodes.putAll( md.borderNodes );
		}

		for ( final RemappedData md : mappedDatas )
		{
			final Edge e = new Edge( md.edges );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				long from = e.from();
				long to = e.to();
				if ( allBorderNodesAssignments.contains( from ) && allBorderNodesAssignments.contains( to ) )
				{
					from = allBorderNodesAssignments.get( from );
					to = allBorderNodesAssignments.get( to );
					e.weight( Double.NaN );
				}

				ae.add( e.weight(), e.affinity(), from, to, e.multiplicity() );
			}
		}

		return new Tuple2<>( key, new OriginalLabelData( allEdges, allCounts, allOutsideNodes, allBorderNodes ) );
	}

}