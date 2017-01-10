package de.hanslovsky.watersheds.rewrite.util;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class NumElements< K > implements PairFunction< Tuple2< K, long[] >, K, Long >
{

	private static final long serialVersionUID = 3708229397749252500L;

	@Override
	public Tuple2< K, Long > call( final Tuple2< K, long[] > t ) throws Exception
	{
		return new Tuple2<>( t._1(), ( long ) t._2().length );
	}



}
