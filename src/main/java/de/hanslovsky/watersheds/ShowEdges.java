package de.hanslovsky.watersheds;

import org.mastodon.collection.ref.RefArrayList;
import org.scijava.ui.behaviour.BehaviourMap;
import org.scijava.ui.behaviour.InputTriggerAdder;
import org.scijava.ui.behaviour.InputTriggerMap;
import org.scijava.ui.behaviour.ScrollBehaviour;
import org.scijava.ui.behaviour.io.InputTriggerConfig;

import bdv.viewer.TriggerBehaviourBindings;
import bdv.viewer.ViewerPanel;
import net.imglib2.algorithm.morphology.watershed.AffinityWatershedBlocked.WeightedEdge;

public class ShowEdges implements ScrollBehaviour
{

	private final RefArrayList< WeightedEdge > edges;

	private int index = 90; // bad edge at index 90?

	private final ViewerPanel viewer;

	private WeightedEdge edge;

	public ShowEdges(
			final RefArrayList< WeightedEdge > edges,
			final ViewerPanel viewer,
			final TriggerBehaviourBindings bindings )
	{
		super();
		this.edges = edges;
		this.viewer = viewer;
		this.index = 0;
		this.edge = null;

		final BehaviourMap behaviourMap = new BehaviourMap();
		final InputTriggerMap inputTriggerMap = new InputTriggerMap();
		final InputTriggerConfig config = viewer.getOptionValues().getInputTriggerConfig();
		final InputTriggerAdder inputAdder = config.inputTriggerAdder( inputTriggerMap, "EDGES" );
		behaviourMap.put( "EDGES", this );
		inputAdder.put( "EDGES", "SPACE scroll" );

		bindings.addBehaviourMap( "EDGES", behaviourMap );
		bindings.addInputTriggerMap( "EDGES", inputTriggerMap );


	}

	public WeightedEdge getEdge()
	{
		return this.edge;
	}

	@Override
	public void scroll( final double wheelRotation, final boolean isHorizontal, final int x, final int y )
	{
		if ( !isHorizontal )
		{
			if ( wheelRotation > 0 )
				index = Math.max( index - 1, 0 );
			else if ( wheelRotation < 0 )
				index = Math.min( index + 1, edges.size() - 1 );

			edge = edges.createRef();
			edges.get( index, edge );
			System.out.println( "Current index is " + index + " " + edge );

			viewer.requestRepaint();

		}
		// TODO Auto-generated method stub

	}

}