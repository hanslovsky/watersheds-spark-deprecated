package de.hanslovsky.watersheds;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

public class Dummy
{
	public static void main( final String[] args )
	{
		final Gson gson = new Gson();
		final long[] longArr = new long[] { 1, 2, 3 };
		final byte[] bytes = new byte[ longArr.length * 8 ];
		final ByteBuffer bb = ByteBuffer.wrap( bytes );
		for ( final long l : longArr )
			bb.putLong( l );
		System.out.println( Arrays.toString( bytes ) );

		final String data = new String( bytes );
		final String jsonData = gson.toJson( data );
		final JsonElement jsonEl = gson.toJsonTree( data );
		System.out.println( jsonEl );
		final String str = gson.fromJson( jsonEl, String.class );
		System.out.println( str );
		System.out.println( Arrays.toString( str.getBytes() ) );
		final ByteBuffer bb2 = ByteBuffer.wrap( str.getBytes() );
		while ( bb2.hasRemaining() )
			System.out.println( bb2.getLong() );

	}
}
