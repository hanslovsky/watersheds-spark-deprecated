package de.hanslovsky.watersheds.graph;

import java.io.Serializable;

public class IdServiceCached implements IdService, Serializable
{

	/**
	 *
	 */
	private static final long serialVersionUID = 5716043843924144377L;

	private final IdService idService;

	private long maxCachedId;

	private final long cacheSize;

	private long currentId;



	public IdServiceCached( final IdService idService, final long cacheSize )
	{
		super();
		this.idService = idService;
		this.cacheSize = cacheSize;
		this.currentId = 0;
		this.maxCachedId = 0;
	}

	@Override
	public long requestIds( final long numIds )
	{
		if ( this.currentId == this.maxCachedId )
		{
			this.currentId = this.idService.requestIds( this.cacheSize );
			this.maxCachedId = this.currentId + this.cacheSize;
		}
		return this.currentId++;
	}

}
