package de.hanslovsky.watersheds.rewrite.preparation;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import de.hanslovsky.watersheds.rewrite.util.HashableLongArray;
import de.hanslovsky.watersheds.rewrite.util.Util;
import gnu.trove.map.hash.TLongLongHashMap;
import scala.Tuple2;

public class OffsetLabels implements
PairFunction< Tuple2< HashableLongArray, Tuple2< long[], long[] > >, HashableLongArray, Tuple2< long[], TLongLongHashMap > >
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
	public Tuple2< HashableLongArray, Tuple2< long[], TLongLongHashMap > >
	call( final Tuple2< HashableLongArray, Tuple2< long[], long[] > > t ) throws Exception
	{
		final long id = Util.positionToIndex( t._1(), dim );
		final long offset = offsets.getValue().get( id );
		final TLongLongHashMap counts = new TLongLongHashMap();
		final long[] data = t._2()._1().clone();
		if ( offset != offsets.getValue().getNoEntryValue() )
		{
			for ( int i = 0; i < data.length; ++i )
				if ( data[ i ] != 0 )
					data[ i ] += offset;
			final long[] c = t._2()._2();
			counts.put( 0, c[ 0 ] );
			for ( int i = 1; i < c.length; ++i )
				counts.put( i + offset, c[ i ] );
		}
		else
			return null;

		return new Tuple2<>( t._1(), new Tuple2<>( data, counts ) );
	}

}
