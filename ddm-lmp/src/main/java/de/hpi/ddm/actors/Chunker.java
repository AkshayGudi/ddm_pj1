package de.hpi.ddm.actors;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.util.ByteString;
import scala.Tuple2;

class Chunker extends GraphStage<FlowShape<ByteString, ByteString>> {
	private final int chunkSize;
	public Inlet<ByteString> in = Inlet.<ByteString>create("Chunker.in");
	public Outlet<ByteString> out = Outlet.<ByteString>create("Chunker.out");
	private FlowShape<ByteString, ByteString> shape = FlowShape.of(in, out);

	public Chunker(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	@Override
	public FlowShape<ByteString, ByteString> shape() {
		return shape;
	}

	@Override
	public GraphStageLogic createLogic(Attributes inheritedAttributes) {
		return new GraphStageLogic(shape) {
			private ByteString buffer = ByteString.empty();
			{
				setHandler(out, new AbstractOutHandler() {
					@Override
					public void onPull() throws Exception {
						if (isClosed(in))
							emitChunk();
						else
							pull(in);
					}
				});
				setHandler(in, new AbstractInHandler() {
					@Override
					public void onPush() throws Exception {
						ByteString elem = grab(in);
						buffer = buffer.concat(elem);
						emitChunk();
					}

					@Override
					public void onUpstreamFinish() throws Exception {
						if (buffer.isEmpty())
							completeStage();
						else {
// There are elements left in buffer, so
// we keep accepting downstream pulls and push from buffer until emptied.
//
// It might be though, that the upstream finished while it was pulled, in which
// case we will not get an onPull from the downstream, because we already had one.
// In that case we need to emit from the buffer.
							if (isAvailable(out))
								emitChunk();
						}
					}
				});
			}

			private void emitChunk() {
				if (buffer.isEmpty()) {
					if (isClosed(in))
						completeStage();
					else
						pull(in);
				} else {
					Tuple2<ByteString, ByteString> split = buffer.splitAt(chunkSize);
					ByteString chunk = split._1();
					buffer = split._2();
					push(out, chunk);
				}
			}
		};
	}
}
