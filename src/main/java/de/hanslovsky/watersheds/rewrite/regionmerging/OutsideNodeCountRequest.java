package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import de.hanslovsky.watersheds.rewrite.graph.edge.Edge;
import de.hanslovsky.watersheds.rewrite.util.Util;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import scala.Tuple2;

public class OutsideNodeCountRequest
{

	public static Logger LOG = LogManager.getLogger( MethodHandles.lookup().lookupClass() );
	static
	{
		LOG.setLevel( Level.INFO );
	}

	public static JavaPairRDD< Long, RemappedData > request( final JavaPairRDD< Long, RemappedData > rdd, final int edgeDataSize )
	{

		final JavaPairRDD< Long, Tuple2< Long, TLongHashSet > > singleRequests = rdd.flatMapToPair( new AssignRequestsToBlockIds() );

		final JavaPairRDD< Long, TLongObjectHashMap< TLongHashSet > > combinedRequestsForEachBlock = singleRequests.aggregateByKey(
				new TLongObjectHashMap< TLongHashSet >(),
				( m, val ) -> putOrAddAll( m, val._1(), val._2() ),
				( m1, m2 ) -> putOrAddAll( m2, m1 ) );

		final JavaPairRDD< Long, ArrayList< Tuple2< TLongLongHashMap, TLongLongHashMap > > > response = rdd
				.join( combinedRequestsForEachBlock )
				.values()
				.flatMapToPair( new HandleRequests() )
				.aggregateByKey( new ArrayList<>(), ( l, v ) -> Util.addAndReturn( l, v ), ( l1, l2 ) -> Util.addAllAndReturn( l1, l2 ) );
		singleRequests.unpersist();

		return rdd.join( response ).mapValues( new ApplyResponse( edgeDataSize ) );
	}

	public static class AssignRequestsToBlockIds implements PairFlatMapFunction< Tuple2< Long, RemappedData >, Long, Tuple2< Long, TLongHashSet > >
	{

		@Override
		public Iterator< Tuple2< Long, Tuple2< Long, TLongHashSet > > > call( final Tuple2< Long, RemappedData > t ) throws Exception
		{
			final TLongObjectHashMap< TLongHashSet > requests = new TLongObjectHashMap<>();
			for ( final TLongLongIterator it = t._2().outsideNodes.iterator(); it.hasNext(); )
			{
				it.advance();
				final long node = it.key();
				final long block = it.value();
				if ( !requests.contains( block ) )
					requests.put( block, new TLongHashSet() );
				requests.get( block ).add( node );

			}
			return new Iterator< Tuple2< Long, Tuple2< Long, TLongHashSet > > >()
			{

				private final TLongObjectIterator< TLongHashSet > requestIt = requests.iterator();

				@Override
				public boolean hasNext()
				{
					return requestIt.hasNext();
				}

				@Override
				public Tuple2< Long, Tuple2< Long, TLongHashSet > > next()
				{
					requestIt.advance();
					return new Tuple2<>( requestIt.key(), new Tuple2<>( t._1(), requestIt.value() ) );
				}
			};
		}
	}

	public static TLongObjectHashMap< TLongHashSet > putOrAddAll( final TLongObjectHashMap< TLongHashSet > m, final long k, final TLongHashSet v )
	{
		if ( m.contains( k ) )
			m.get( k ).addAll( v );
		else
			m.put( k, v );
		return m;
	}

	public static TLongObjectHashMap< TLongHashSet > putOrAddAll( final TLongObjectHashMap< TLongHashSet > source, final TLongObjectHashMap< TLongHashSet > target )
	{
		for ( final TLongObjectIterator< TLongHashSet > it = source.iterator(); it.hasNext(); )
		{
			it.advance();
			putOrAddAll( target, it.key(), it.value() );
		}
		return target;
	}

	public static class HandleRequests implements
	PairFlatMapFunction< Tuple2< RemappedData, TLongObjectHashMap< TLongHashSet > >, Long, Tuple2< TLongLongHashMap, TLongLongHashMap > >
	{

		@Override
		public Iterator< Tuple2< Long, Tuple2< TLongLongHashMap, TLongLongHashMap > > > call( final Tuple2< RemappedData, TLongObjectHashMap< TLongHashSet > > dataAndRequests ) throws Exception
		{
			final TLongLongHashMap counts = dataAndRequests._1().counts;
			final TLongLongHashMap borderNodeMap = dataAndRequests._1().borderNodeMappings;
			final TLongObjectHashMap< Tuple2< TLongLongHashMap, TLongLongHashMap > > result = new TLongObjectHashMap<>();
			final TLongObjectHashMap< TLongHashSet > requests = dataAndRequests._2();
			for ( final TLongObjectIterator< TLongHashSet > it = requests.iterator(); it.hasNext(); )
			{
				it.advance();
				final TLongLongHashMap countsForBlock = new TLongLongHashMap();
				final TLongLongHashMap borderNodeMapForBlock = new TLongLongHashMap();
				for ( final TLongIterator req = it.value().iterator(); req.hasNext(); )
				{
					final long id = req.next();
					final long mappedId = borderNodeMap.get( id );
					countsForBlock.put( mappedId, counts.get( mappedId ) );
					borderNodeMapForBlock.put( id, mappedId );
				}
				result.put( it.key(), new Tuple2<>( borderNodeMapForBlock, countsForBlock ) );

			}

			return new Iterator< Tuple2< Long, Tuple2< TLongLongHashMap, TLongLongHashMap > > >()
			{

				private final TLongObjectIterator< Tuple2< TLongLongHashMap, TLongLongHashMap > > it = result.iterator();

				@Override
				public boolean hasNext()
				{
					return it.hasNext();
				}

				@Override
				public Tuple2< Long, Tuple2< TLongLongHashMap, TLongLongHashMap > > next()
				{
					it.advance();
					return new Tuple2<>( it.key(), it.value() );
				}

			};
		}

	}

	public static class ApplyResponse implements
	Function< Tuple2< RemappedData, ArrayList< Tuple2< TLongLongHashMap, TLongLongHashMap > > >, RemappedData >
	{

		private final int edgeDataSize;

		public ApplyResponse( final int edgeDataSize )
		{
			super();
			this.edgeDataSize = edgeDataSize;
		}

		@Override
		public RemappedData call( final Tuple2< RemappedData, ArrayList< Tuple2< TLongLongHashMap, TLongLongHashMap > > > t ) throws Exception
		{
			final TLongLongHashMap counts = t._1().counts;
			LOG.debug( "Merging " + counts.size() + " counts into block." );

			final TLongLongHashMap borderNodeAssignments = new TLongLongHashMap();

			for ( final Tuple2< TLongLongHashMap, TLongLongHashMap > cts : t._2() )
			{
				final TLongLongHashMap bna = cts._1();
				for ( final TLongLongIterator it = bna.iterator(); it.hasNext(); )
				{
					it.advance();
					final long k = it.key();
					final long v = it.value();
					if ( k != v )
						counts.remove( k );
				}
				borderNodeAssignments.putAll( bna );
				counts.putAll( cts._2() );
			}

			final Edge e = new Edge( t._1().edges, edgeDataSize );
			for ( int i = 0; i < e.size(); ++i )
			{
				e.setIndex( i );
				final long from = e.from();
				final long to = e.to();

				if ( borderNodeAssignments.contains( from ) )
					e.from( borderNodeAssignments.get( from ) );

				if ( borderNodeAssignments.contains( to ) )
					e.to( borderNodeAssignments.get( to ) );
			}

			final TLongLongHashMap outsideNodes = t._1().outsideNodes;
			final TLongLongHashMap outsideNodesMapped = new TLongLongHashMap();
			for ( final TLongLongIterator it = outsideNodes.iterator(); it.hasNext(); )
			{
				it.advance();
				final long k = it.key();
				if ( !borderNodeAssignments.contains( k ) )
					throw new RuntimeException( "Outside node " + k + "not contained!" );
				outsideNodesMapped.put( borderNodeAssignments.get( k ), it.value() );
			}

			return new RemappedData( t._1().edges, counts, outsideNodesMapped, t._1().merges, t._1().borderNodeMappings );
		}

	}

}
