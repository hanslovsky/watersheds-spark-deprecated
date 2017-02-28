package de.hanslovsky.watersheds.rewrite.regionmerging;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import de.hanslovsky.watersheds.rewrite.graph.Edge;
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
		LOG.setLevel( Level.TRACE );
	}

	public static JavaPairRDD< Long, RemappedData > request( final JavaPairRDD< Long, RemappedData > rdd )
	{

		final JavaPairRDD< Long, Tuple2< Long, TLongHashSet > > blub = rdd
				.flatMapToPair( t -> {
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
//					if ( t._1().longValue() == 15 )
//						System.out.println( "REQUESTS? " + requests );
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
				} );

		final JavaPairRDD< Long, TLongObjectHashMap< TLongHashSet > > requestsRDD = blub.aggregateByKey(
				new TLongObjectHashMap< TLongHashSet >(),
				( m, val ) -> {
					final long k = val._1();
					if ( !m.contains( k ) )
						m.put( k, val._2() );
					else
						m.get( k ).addAll( val._2() );
					return m;
				},
				( m1, m2 ) -> {
					for ( final TLongObjectIterator< TLongHashSet > it = m2.iterator(); it.hasNext(); )
					{
						it.advance();
						final long k = it.key();
						if ( m1.contains( k ) )
							m1.get( k ).addAll( it.value() );
						else
							m1.put( k, it.value() );
					}
					return m1;
				} );

		final JavaPairRDD< Long, ArrayList< Tuple2< TLongLongHashMap, TLongLongHashMap > > > response = rdd
				.join( requestsRDD )
//				.values()
				.flatMapToPair( tt -> {
					final Tuple2< RemappedData, TLongObjectHashMap< TLongHashSet > > t = tt._2();
					final TLongLongHashMap counts = t._1().counts;
					final TLongLongHashMap borderNodeMap = t._1().borderNodeMappings;
					final TLongObjectHashMap< Tuple2< TLongLongHashMap, TLongLongHashMap > > result = new TLongObjectHashMap<>();
					for ( final TLongObjectIterator< TLongHashSet > it = t._2().iterator(); it.hasNext(); )
					{
						it.advance();
						final TLongLongHashMap countsForBlock = new TLongLongHashMap();
						final TLongLongHashMap borderNodeMapForBlock = new TLongLongHashMap();
						for ( final TLongIterator req = it.value().iterator(); req.hasNext(); )
						{
							final long id = req.next();
							final long mappedId = borderNodeMap.get( id );
//							if ( !borderNodeMap.contains( id ) )
//								throw new RuntimeException( "Why does borderNodeMpa not contain " + id );
							countsForBlock.put( mappedId, counts.get( mappedId ) );
							borderNodeMapForBlock.put( id, mappedId );
						}
//						if ( it.key() == 15 )
//							System.out.println( "HANDLING REQUEST: " + tt._1() + " " + borderNodeMapForBlock + " " + countsForBlock + " " + borderNodeMap );
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
				} )
				.aggregateByKey(
						new ArrayList<>(),
						( l, v ) -> {
							l.add( v );
							return l;
						},
						( l1, l2 ) -> {
							l1.addAll( l2 );
							return l1;
						} );
		blub.unpersist();

		return rdd
				.join( response )
				.mapToPair( t -> {
					final TLongLongHashMap counts = t._2()._1().counts;
					LOG.debug( "Merging " + counts.size() + " counts into block." );

					final TLongLongHashMap borderNodeAssignments = new TLongLongHashMap();

					for ( final Tuple2< TLongLongHashMap, TLongLongHashMap > cts : t._2()._2() )
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

					final Edge e = new Edge( t._2()._1().edges );
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

					final TLongLongHashMap outsideNodes = t._2()._1().outsideNodes;
					final TLongLongHashMap outsideNodesMapped = new TLongLongHashMap();
					for ( final TLongLongIterator it = outsideNodes.iterator(); it.hasNext(); )
					{
						it.advance();
						final long k = it.key();
						if ( !borderNodeAssignments.contains( k ) )
							throw new RuntimeException( "Outside node " + k + "not contained!" );
						outsideNodesMapped.put( borderNodeAssignments.get( k ), it.value() );
					}

					return new Tuple2<>( t._1(), new RemappedData( t._2()._1().edges, counts, outsideNodesMapped, t._2()._1().merges, t._2()._1().borderNodeMappings ) );
//					return new Tuple2<>( t._1(), t._2()._1() );
				} );
	}

}
