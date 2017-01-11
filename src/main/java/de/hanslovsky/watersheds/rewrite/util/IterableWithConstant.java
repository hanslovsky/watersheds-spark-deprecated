package de.hanslovsky.watersheds.rewrite.util;

import java.util.Iterator;

import scala.Tuple2;

public class IterableWithConstant< V, C > implements Iterable< Tuple2< V, C > >
{

	private final Iterable< V > iterable;

	private final C c;

	public IterableWithConstant( final Iterable< V > iterable, final C c )
	{
		super();
		this.iterable = iterable;
		this.c = c;
	}

	public class IteratorWithConstant implements Iterator< Tuple2< V, C > >
	{

		private final Iterator< V > iterator = iterable.iterator();

		@Override
		public boolean hasNext()
		{
			return iterator.hasNext();
		}

		@Override
		public Tuple2< V, C > next()
		{
			return new Tuple2<>( iterator.next(), c );
		}

	}

	@Override
	public Iterator< Tuple2< V, C > > iterator()
	{
		return new IteratorWithConstant();
	}

}
