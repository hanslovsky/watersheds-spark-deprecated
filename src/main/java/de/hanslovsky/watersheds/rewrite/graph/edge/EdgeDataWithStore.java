package de.hanslovsky.watersheds.rewrite.graph.edge;

import gnu.trove.list.array.TLongArrayList;

public class EdgeDataWithStore
{

	private int pos;

	private final int stride;

	private final TLongArrayList data;

	public EdgeDataWithStore( final int stride, final TLongArrayList data )
	{
		super();
		this.stride = stride;
		this.data = data;
		this.pos = 0;
	}

	public TLongArrayList getData()
	{
		return data;
	}

	public void setPos( final int pos )
	{
		this.pos = stride * pos;
	}

	public void add( final EdgeDataWithStore d )
	{
		for ( int i = 0, p = d.pos; i < this.stride; ++i, ++p )
			this.data.add( d.data.get( p ) );
	}

}
