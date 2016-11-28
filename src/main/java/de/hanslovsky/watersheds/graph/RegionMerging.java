package de.hanslovsky.watersheds.graph;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.MultiGraph;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import de.hanslovsky.watersheds.graph.MergeBloc.EdgeMerger;
import de.hanslovsky.watersheds.graph.MergeBloc.Function;
import de.hanslovsky.watersheds.graph.MergeBloc.IdService;
import de.hanslovsky.watersheds.graph.MergeBloc.In;
import de.hanslovsky.watersheds.graph.MergeBloc.MergerService;
import de.hanslovsky.watersheds.graph.MergeBloc.Out;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class RegionMerging
{

	private final MergeBloc.Function f;

	private final MergeBloc.EdgeMerger merger;

	private final IdService idService;

	private final MergerService mergerService;

	public RegionMerging( final Function f, final EdgeMerger merger, final IdService idService, final MergerService mergerService )
	{
		super();
		this.f = f;
		this.merger = merger;
		this.idService = idService;
		this.mergerService = mergerService;
	}

	public JavaPairRDD< Long, MergeBloc.In > run( final JavaSparkContext sc, final JavaPairRDD< Long, MergeBloc.In > rddIn, final double threshold )
	{
		final List< Long > indices = rddIn.keys().collect();
		final int[] parents = new int[ indices.size() ];
		for ( final Long m : indices )
			parents[ m.intValue() ] = m.intValue();
		final DisjointSets dj = new DisjointSets( parents, new int[ parents.length ], parents.length );

		JavaPairRDD< Long, MergeBloc.In > rdd = rddIn;

//		final boolean[] isDone = new boolean[ parents.length ];

		for ( boolean hasChanged = true; hasChanged; )
		{

			final JavaPairRDD< Tuple2< Long, Long >, MergeBloc.Out > mergedEdges =
					rdd.mapToPair( new MergeBloc.MergeBlocPairFunction2( f, merger, threshold, idService, mergerService ) ).cache();

			System.out.println( "Merging edges!" );
			final List< Tuple2< Long, Long > > mergers = mergedEdges.keys().collect();
			System.out.println( "Done merging edges!" );
			boolean pointsOutside = false;
			System.out.println( "MERGERS! " + mergers );
			for ( final Tuple2< Long, Long > m : mergers )
			{
				System.out.println( m );
				if ( m._2() == -1 )
					continue;
				if ( m._1() != m._2() )
					pointsOutside = true;
				final int r1 = dj.findRoot( m._1().intValue() );
				final int r2 = dj.findRoot( m._2().intValue() );
				dj.join( r1, r2 );
			}

			hasChanged = pointsOutside;

			final Broadcast< int[] > parentsBC = sc.broadcast( parents );

			System.out.println( Arrays.toString( parents ) );

			final JavaPairRDD< Long, Out > rdd1 = mergedEdges.mapToPair( new SetKeyToRoot<>( parentsBC ) );
			rdd1.count();
			final JavaPairRDD< Long, Tuple2< Long, Out > > rdd2 = rdd1.mapToPair( new AddKeyToValue<>() );
			rdd2.count();
			final JavaPairRDD< Long, Tuple2< Long, Out > > rdd3 = rdd2.reduceByKey( new MergeByKey( parentsBC ) );
			rdd3.count();
			final JavaPairRDD< Long, Out > rdd4 = rdd3.mapToPair( new RemoveFromValue<>() );
			rdd4.count();
			final JavaPairRDD< Long, In > rdd5 = rdd4.mapToPair( new UpdateEdgesAndCounts() );
			rdd5.count();
			rdd = rdd5.cache();
			rdd.count();

//			rdd = mergedEdges
//					.mapToPair( new SetKeyToRoot<>( parentsBC ) )
//					.mapToPair( new AddKeyToValue<>() )
//					.reduceByKey( new MergeByKey( parentsBC ) )
//					.mapToPair( new RemoveFromValue<>() )
//					.mapToPair( new UpdateEdgesAndCounts() )
//					.cache();
//			rdd.count();
			System.out.print( "Updated rdd " + hasChanged + " " + pointsOutside );
//			System.exit( 1 );

			// assume zero based, contiguuous indices?

		}

		return rdd;
	}

	public static class CountOverSquaredSize implements Function, Serializable
	{
		/**
		 *
		 */
		private static final long serialVersionUID = -2382379605891324313L;

		@Override
		public double weight( final double a, final long c1, final long c2 )
		{
			return Math.min( c1, c2 ) / ( a * a );
		}

	}

	public static void main( final String[] args ) throws InterruptedException
	{
		final SparkConf conf = new SparkConf().setAppName( "mergin" ).setMaster( "local[*]" );
		final JavaSparkContext sc = new JavaSparkContext( conf );

		final ArrayList< Tuple2< Long, MergeBloc.In > > al = new ArrayList<>();
		{
			final TDoubleArrayList affinities = new TDoubleArrayList( new double[] {
					Double.NaN, 0.9, Edge.ltd( 10 ), Edge.ltd( 11 ), 1,
					Double.NaN, 0.1, Edge.ltd( 11 ), Edge.ltd( 12 ), 1,
					Double.NaN, 0.1, Edge.ltd( 13 ), Edge.ltd( 12 ), 1,
					Double.NaN, 0.4, Edge.ltd( 13 ), Edge.ltd( 10 ), 1,
					Double.NaN, 0.8, Edge.ltd( 11 ), Edge.ltd( 13 ), 1,
					Double.NaN, 0.9, Edge.ltd( 11 ), Edge.ltd( 14 ), 1,
					Double.NaN, 0.2, Edge.ltd( 10 ), Edge.ltd( 15 ), 1
			} );

			final TLongLongHashMap counts = new TLongLongHashMap(
					new long[] { 10, 11, 12, 13, 14, 15 },
					new long[] { 15, 20, 1, 2, 15, 4000 } );

//			final long[] counts = new long[] {
//					10, 15,
//					11, 20,
//					12, 1,
//					13, 2,
//			};

			final TLongLongHashMap assignments = new TLongLongHashMap(
					new long[] { 10, 11, 12, 13 },
					new long[] { 10, 11, 12, 13 } );

//			final long[] assignments = new long[] {
//					10, 10,
//					11, 11,
//					12, 12,
//					13, 13
//			};

			final TLongLongHashMap outside = new TLongLongHashMap(
					new long[] { 14, 15 },
					new long[] { 1, 2 } );

//			final long[] outside = new long[] {
//					14, 15, 1,
//					15, 4000, 2
//			};
			al.add( new Tuple2<>( 0l, new MergeBloc.In( affinities, counts, outside ) ) );
		}

		{
			final TDoubleArrayList affinities = new TDoubleArrayList( new double[] {
					Double.NaN, 0.9, Edge.ltd( 11 ), Edge.ltd( 14 ), 1,
					Double.NaN, 1.0, Edge.ltd( 14 ), Edge.ltd( 16 ), 1,
			} );

			final TLongLongHashMap counts = new TLongLongHashMap(
					new long[] { 14, 16, 11 },
					new long[] { 15, 30, 20 } );
//			final long[] counts = new long[] {
//					14, 15,
//					16, 30
//			};

			final TLongLongHashMap assignments = new TLongLongHashMap(
					new long[] { 14, 16 },
					new long[] { 14, 16 } );
//			final long[] assignments = new long[] {
//					14, 14,
//					16, 16
//			};

			final TLongLongHashMap outside = new TLongLongHashMap( new long[] { 11 }, new long[] { 0 } );
//			final long[] outside = new long[] {
//					11, 20, 0
//			};
			al.add( new Tuple2<>( 1l, new MergeBloc.In( affinities, counts, outside ) ) );
		}

		{
			final TDoubleArrayList affinities = new TDoubleArrayList( new double[] {
					Double.NaN, 0.2, Edge.ltd( 10 ), Edge.ltd( 15 ), 1
			} );

			final TLongLongHashMap counts = new TLongLongHashMap(
					new long[] { 15, 10 },
					new long[] { 4000, 15 } );
//			final long[] counts = new long[] {
//					15, 4000
//			};

			final TLongLongHashMap assignments = new TLongLongHashMap( new long[] { 15 }, new long[] { 15 } );
//			final long[] assignments = new long[] {
//					15, 15
//			};

			final TLongLongHashMap outside = new TLongLongHashMap( new long[] { 10 }, new long[] { 0 } );
//			final long[] outside = new long[] {
//					10, 15, 0
//			};
			al.add( new Tuple2<>( 2l, new MergeBloc.In( affinities, counts, outside ) ) );
		}

		final JavaPairRDD< Long, MergeBloc.In > rdd = sc.parallelizePairs( al );
		final EdgeMerger em = MergeBloc.DEFAULT_EDGE_MERGER;

		final AtomicLong currentId = new AtomicLong( 17 );
		final Context ctx = ZMQ.context( 1 );
		final Socket idSocket = ctx.socket( ZMQ.REP );
		final String idServiceAddr = "ipc://IdService";
		idSocket.bind( idServiceAddr );
		final Thread t = new Thread( () -> {
			while ( !Thread.currentThread().isInterrupted() )
			{
				final byte[] msg = idSocket.recv();
				System.out.println( "Received msg " + Arrays.toString( msg ) );
				final ByteBuffer bb = ByteBuffer.wrap( msg );
				if ( msg.length == 0 )
					continue;
				final long id = currentId.getAndAdd( bb.getLong() );
				bb.rewind();
				bb.putLong( id );
				System.out.println( "Sending msg " + Arrays.toString( msg ) );
				idSocket.send( msg, 0 );
				System.out.println( "Sent msg " + Arrays.toString( msg ) );
			}
		} );
		t.start();

		final IdService idService = ( IdService & Serializable ) numIds -> {
			final Context ctx2 = ZMQ.context( 1 );
			final Socket socket = ctx2.socket( ZMQ.REQ );
			socket.connect( idServiceAddr );
			final byte[] data = new byte[ Long.BYTES ];
			final ByteBuffer bb = ByteBuffer.wrap( data );
			bb.putLong( numIds );
			socket.send( data, 0 );
			final byte[] msg = socket.recv();
			final long id = ByteBuffer.wrap( msg ).getLong();
			socket.close();
			ctx2.close();
			System.out.println( "Closed ctx2" );
			return id;
		};

		final TLongArrayList merges = new TLongArrayList();
		final MergerService mergerService = ( MergerService & Serializable ) ( n1, n2, n, w ) -> {
//			merges.add( n1 );
//			merges.add( n2 );
//			merges.add( n );
//			merges.add( Double.doubleToLongBits( w ) );
		};

		final RegionMerging rm = new RegionMerging(
				new CountOverSquaredSize(), // ( Function & Serializable ) ( a,
				// c1, c2 ) -> Math.min( c1, c2 ) /
				// ( a * a ),
				em,
				idService,
				mergerService );

		final List< Tuple2< Long, MergeBloc.In > > result = rm.run( sc, rdd, 99 ).collect();

		System.out.println( "Closing sc" );
		sc.close();
//		sc.wait();

		t.interrupt();
		final Socket sk = ctx.socket( ZMQ.REQ );
		sk.connect( idServiceAddr );
		sk.send( new byte[ 0 ], 0 );
		System.out.println( "SEND END MSG" );
		t.join();
		System.out.println( "Waiting for thread to join" );
		sk.close();
		ctx.close();

		System.out.println( result );

		final MultiGraph g = new MultiGraph( "graph" );

		final TLongHashSet roots = new TLongHashSet();
		final TLongLongHashMap parents = new TLongLongHashMap();
		for ( final Tuple2< Long, MergeBloc.In > r : result )
		{
//			System.out.println( r._1() + " " + r._2().assignments );
//			for ( final TLongLongIterator it = r._2().assignments.iterator(); it.hasNext(); )
//			{
//				it.advance();
//				parents.put( it.key(), it.value() );
//			}
		}
		for ( final TLongLongIterator it = parents.iterator(); it.hasNext(); )
		{
			it.advance();
			final Node n = g.addNode( "" + it.key() );
			n.addAttribute( "ui.label", "" + it.key() );
		}

		for ( final TLongLongIterator it = parents.iterator(); it.hasNext(); )
		{
			it.advance();
			System.out.println( "Adding edge " + it.key() + " " + it.value() );
//			g.addEdge( id, from, to, directed )
			g.addEdge( it.key() + "-" + it.value(), "" + it.key(), "" + it.value(), true );
		}

		g.display( true );



	}

	public static class SetKeyToRoot< V > implements PairFunction< Tuple2< Tuple2< Long, Long >, V >, Long, V >
	{

		private static final long serialVersionUID = -6206815670426308405L;

		private final Broadcast< int[] > parents;

		public SetKeyToRoot( final Broadcast< int[] > parents )
		{
			super();
			this.parents = parents;
		}

		@Override
		public Tuple2< Long, V > call( final Tuple2< Tuple2< Long, Long >, V > t ) throws Exception
		{
			final long k = parents.getValue()[ t._1()._1().intValue() ];
			return new Tuple2<>( k, t._2() );
		}

	}

	public static class AddKeyToValue< K, V > implements PairFunction< Tuple2< K, V >, K, Tuple2< K, V > >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = -8129563585405199540L;

		@Override
		public Tuple2< K, Tuple2< K, V > > call( final Tuple2< K, V > t ) throws Exception
		{
			return new Tuple2<>( t._1(), t );
		}

	}

	public static class RemoveFromValue< K, V1, V2 > implements PairFunction< Tuple2< K, Tuple2< V1, V2 > >, K, V2 >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = -5495876386051788944L;

		@Override
		public Tuple2< K, V2 > call( final Tuple2< K, Tuple2< V1, V2 > > t ) throws Exception
		{
			// TODO Auto-generated method stub
			return new Tuple2<>( t._1(), t._2()._2() );
		}


	}

	public static class MergeByKey implements Function2< Tuple2< Long, MergeBloc.Out >, Tuple2< Long, MergeBloc.Out >, Tuple2< Long, MergeBloc.Out > >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 1428628232921208948L;
		private final Broadcast< int[] > parents;

		public MergeByKey( final Broadcast< int[] > parents )
		{
			super();
			this.parents = parents;
		}

		@Override
		public Tuple2< Long, MergeBloc.Out > call(
				final Tuple2< Long, MergeBloc.Out > eacWithKey1,
				final Tuple2< Long, MergeBloc.Out > eacWithKey2 ) throws Exception
		{
			int ABC = 0;
			System.out.println( "MergeByKey " + ++ABC );
			if ( eacWithKey1._1().longValue() == eacWithKey2._1().longValue() )
			{
				System.out.println( "RETURNING FOR SAME KEY " + eacWithKey1._1() + " " + eacWithKey1._2().assignments );
				return eacWithKey1;
			}

			System.out.println( "MergeByKey " + ++ABC );

			final int[] parents = this.parents.getValue();
			final int r = parents[ eacWithKey1._1().intValue() ];
			final MergeBloc.Out eac1 = eacWithKey1._2();
			final MergeBloc.Out eac2 = eacWithKey2._2();
			final TLongLongHashMap assignments = new TLongLongHashMap();
			assignments.putAll( eac1.assignments );
			assignments.putAll( eac2.assignments );
			System.out.println( "MergeByKey " + ++ABC );
//			final long[] assignments = new long[ eac1.assignments.length + eac2.assignments.length ];
//			System.arraycopy( eac1.assignments, 0, assignments, 0, eac1.assignments.length );
//			System.arraycopy( eac2.assignments, 0, assignments, eac1.assignments.length, eac2.assignments.length );

//			final double[] edges = new double[ eac1.edges.size() + eac2.edges.size() ];
//			System.arraycopy( eac1.edges, 0, edges, 0, eac1.edges.size() );
//			System.arraycopy( eac2.edges, 0, edges, eac1.edges.size(), eac2.edges.size() );
			final TDoubleArrayList edges = eac1.edges;
			edges.addAll( eac2.edges );

			final TLongLongHashMap counts = new TLongLongHashMap();
			addCounts( eac1.counts, counts, eac1.outside );
			addCounts( eac2.counts, counts, eac2.outside );
			System.out.println( "MergeByKey " + ++ABC );

//			final long[] counts = new long[ eac1.counts.length + eac2.counts.length ];
//			System.arraycopy( eac1.counts, 0, counts, 0, eac1.counts.length );
//			System.arraycopy( eac2.counts, 0, counts, eac1.counts.length, eac2.counts.length );

			final TLongLongHashMap outside = new TLongLongHashMap();
			System.out.println( eacWithKey1._1() + " " + eacWithKey2._1() );
			System.out.println( eac1.outside );
			System.out.println( eac2.outside );
			System.out.println( "WAS IST HIER LOS??" );
			System.out.flush();
			addOutside( eac1.outside, outside, r, parents );
			addOutside( eac2.outside, outside, r, parents );
			System.out.println( "MergeByKey " + ++ABC );
//			for ( int i = 0; i < eac1.outside.length; i += EdgesAndCounts.OUTSIDE_STEP )
//			{
//				if ( parents[ ( int ) eac1.outside[ i + 2 ] ] == r )
//					continue;
//				outside.add( eac1.outside[ i ] );
//				outside.add( eac1.outside[ i + 1 ] );
//				outside.add( eac1.outside[ i + 2 ] );
//			}
//			for ( int i = 0; i < eac2.outside.length; i += EdgesAndCounts.OUTSIDE_STEP )
//			{
//				if ( parents[ ( int ) eac2.outside[ i + 2 ] ] == r )
//					continue;
//				outside.add( eac2.outside[ i ] );
//				outside.add( eac2.outside[ i + 1 ] );
//				outside.add( eac2.outside[ i + 2 ] );
//			}

			if ( assignments.size() == 0 )
			{
				System.out.println( eacWithKey1._1() + " " + eacWithKey2._1() );
				System.out.println( eacWithKey1._2().assignments + " " + eacWithKey2._2().assignments );
				System.exit( 123 );
			}

			return new Tuple2<>( eacWithKey1._1(), new MergeBloc.Out( edges, counts, outside, assignments ) );
		}

	}

	public static class UpdateEdgesAndCounts implements PairFunction< Tuple2< Long, MergeBloc.Out >, Long, MergeBloc.In >
	{

		/**
		 *
		 */
		private static final long serialVersionUID = 2239780753041141629L;

		@Override
		public Tuple2< Long, MergeBloc.In > call( final Tuple2< Long, MergeBloc.Out > t ) throws Exception
		{
			final TDoubleArrayList edges = t._2().edges;
			final TLongLongHashMap assignments = t._2().assignments;
			for ( int i = 0; i < edges.size(); i += Edge.SIZE )
			{
				final int fromIndex = i + 2;
				final int toIndex = i + 3;
				edges.set( fromIndex, Edge.ltd( assignments.get( Edge.dtl( edges.get( fromIndex ) ) ) ) );
				edges.set( toIndex, Edge.ltd( assignments.get( Edge.dtl( edges.get( toIndex ) ) ) ) );
//				edges[ fromIndex ] = Edge.ltd( assignments.get( Edge.dtl( edges[ fromIndex ] ) ) );
//				edges[ toIndex ] = Edge.ltd( assignments.get( Edge.dtl( edges[ toIndex ] ) ) );
			}

			final TLongLongHashMap cleanAssignments = new TLongLongHashMap();
			for ( final TLongLongIterator it = assignments.iterator(); it.hasNext(); )
			{
				it.advance();
				final long k = it.key();
				final long v = it.value();
				if ( k == v )
					cleanAssignments.put( k, v );
			}

			final MergeBloc.In result = new MergeBloc.In( edges, t._2().counts, t._2().outside );
//			System.out.println( result.edges );
			System.out.println( "Updating..." );
			System.out.println( result.counts );
			System.out.println( result.outside );
//			System.out.println( result.assignments );
			System.out.println( assignments );
			System.out.flush();

			return new Tuple2<>( t._1(), result );
		}

	}

	public static void addCounts( final TLongLongHashMap source, final TLongLongHashMap target, final TLongLongHashMap outside )
	{
		for ( final TLongLongIterator it = source.iterator(); it.hasNext(); )
		{
			it.advance();
			final long k = it.key();
			if ( !outside.contains( k ) )
				target.put( k, it.value() );
		}
	}

	public static void addOutside( final TLongLongHashMap source, final TLongLongHashMap target, final int root, final int[] parents )
	{
		for ( final TLongLongIterator it = source.iterator(); it.hasNext(); )
		{
			final long v = it.value();
			if ( parents[ ( int ) v ] == root )
				continue;
			target.put( it.key(), v );
		}
	}
}
