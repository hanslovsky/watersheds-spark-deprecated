package de.hanslovsky.watersheds.rewrite.util;

import java.io.Serializable;
import java.util.Arrays;

public class HashableLongArray implements Serializable
{

	private static final long serialVersionUID = 2535960634723267306L;

	private final long[] data;

	private final int hash;

	public HashableLongArray( final long... data )
	{
		super();
		this.data = data;
		this.hash = Arrays.hashCode( this.data );
	}

	public long[] getData()
	{
		return data;
	}

	@Override
	public int hashCode()
	{
		return hash;
	}

	@Override
	public boolean equals( final Object o )
	{
		return o instanceof HashableLongArray ? Arrays.equals( ( ( HashableLongArray ) o ).data, data ) : false;
	}

}
