package de.hanslovsky.watersheds;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import gnu.trove.map.hash.TLongLongHashMap;
import scala.Tuple2;
import scala.Tuple3;

public class OffsetLabels implements
PairFunction< Tuple2< Tuple3< Long, Long, Long >, long[] >, Tuple3< Long, Long, Long >, long[] >
{

	private static final long serialVersionUID = -104948149699827832L;

	private final Broadcast< TLongLongHashMap > offsets;

	private final long[] dim;

	public OffsetLabels( final Broadcast< TLongLongHashMap > offsets, final long[] dim )
	{
		super();
		this.offsets = offsets;
		this.dim = dim;
	}

	@Override
	public Tuple2< Tuple3< Long, Long, Long >, long[] >
	call( final Tuple2< Tuple3< Long, Long, Long >, long[] > t ) throws Exception
	{
		final long id = Util.positionToIndex( t._1(), dim );
		final long offset = offsets.getValue().get( id );
		if ( offset != offsets.getValue().getNoEntryValue() )
		{
			final long[] data = t._2();
			for ( int i = 0; i < data.length; ++i )
				if ( data[ i ] != 0 )
					data[ i ] += offset;
		}
		return t;
	}

}
