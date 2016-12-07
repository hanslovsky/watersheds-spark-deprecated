package de.hanslovsky.watersheds.graph;

import gnu.trove.list.array.TDoubleArrayList;

public class Edge
{

	public static int SIZE = 5;

	private final TDoubleArrayList data;

	private int k;

	public Edge( final TDoubleArrayList data )
	{
		super();
		this.data = data;
	}

	public int size()
	{
		return data.size() / SIZE;
	}

	public void setIndex( final int k )
	{
		this.k = SIZE * k;
	}

	public double weight()
	{
		return data.get( k );
	}

	public void weight( final double weight )
	{
		data.set( k, weight );
	}

	public double affinity()
	{
		return data.get( k + 1 );
	}

	public void affinity( final double affinity )
	{
		data.set( k + 1, affinity );
	}

	public long from()
	{
		return dtl( data.get( k + 2 ) );
	}

	public void from( final long from )
	{
		data.set( k + 2, ltd( from ) );
	}

	public long to()
	{
		return dtl( data.get( k + 3 ) );
	}

	public void to( final long to )
	{
		data.set( k + 3, ltd( to ) );
	}

	public long multiplicity()
	{
		return dtl( data.get( k + 4 ) );
	}

	public void multiplicity( final long multiplicity )
	{
		data.set( k + 4, ltd( multiplicity ) );
	}

	public int add( final double weight, final double affinity, final long from, final long to, final long multiplicity )
	{
		final int index = size();
		data.add( weight );
		data.add( affinity );
		data.add( ltd( from ) );
		data.add( ltd( to ) );
		data.add( ltd( multiplicity ) );
		return index;
	}

	public TDoubleArrayList data()
	{
		return this.data;
	}

	public static double ltd( final long l )
	{
		return Double.longBitsToDouble( l );
	}

	public static long dtl( final double d )
	{
		return Double.doubleToLongBits( d );
	}

}
