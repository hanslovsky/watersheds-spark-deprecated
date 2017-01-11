package de.hanslovsky.watersheds.rewrite.regionmerging;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.rewrite.mergebloc.MergeBlocOut;
import net.imglib2.algorithm.morphology.watershed.DisjointSets;
import scala.Tuple2;

public class FindRootBlock implements PairFunction< Tuple2< Long, Tuple2< Long, MergeBlocOut > >, Long, Tuple2< Long, MergeBlocOut > >
{

	private final Broadcast< int[] > parentsBC;

	private final int setCount;

	public FindRootBlock( final Broadcast< int[] > parentsBC, final int setCount )
	{
		super();
		this.parentsBC = parentsBC;
		this.setCount = setCount;
	}

	@Override
	public Tuple2< Long, Tuple2< Long, MergeBlocOut > > call( final Tuple2< Long, Tuple2< Long, MergeBlocOut > > t ) throws Exception
	{
		final int[] p = parentsBC.getValue();
		final int otherKey = new DisjointSets( p, new int[ p.length ], setCount ).findRoot( t._2()._1().intValue() );
		System.out.println( t._1() + " is rooted at " + otherKey );
		return new Tuple2<>( t._1(), new Tuple2<>( ( long ) otherKey, t._2()._2() ) );
	}

}