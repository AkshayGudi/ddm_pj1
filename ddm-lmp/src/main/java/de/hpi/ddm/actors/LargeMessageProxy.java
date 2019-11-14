package de.hpi.ddm.actors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.japi.function.Creator;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializers;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "largeMessageProxy";
	
	public static Props props() {
		return Props.create(LargeMessageProxy.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class CustomMessage implements Serializable {
		private static final long serialVersionUID = 2237807743872319842L;
		private byte[] byteData;
		private ActorRef senderActor;
		private ActorRef receiverActor;
		private Integer serializerID;
		private String manifest;
	}
	
	/////////////////
	// Actor State //
	/////////////////
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(LargeMessage.class, this::handle)
				.match(BytesMessage.class, this::handle)
				.match(ByteString.class, this::handle)
				
				.matchEquals("ack", msg -> {
					log().info("got value");
				})
				.matchEquals("init", msg -> {
					log().info("Stream initialized");
					sender().tell("ack", self());
				}).matchEquals("done", completed -> {
					log().info("Stream completed");
				}).match(StreamFailure.class, failed -> {
					log().error(failed.getCause(), "Stream failed!");
				})
				
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	

	static class StreamFailure {
		private final Throwable cause;

		public StreamFailure(Throwable cause) {
			this.cause = cause;
		}

		public Throwable getCause() {
			return cause;
		}
	}
	
	private void handle(LargeMessage<?> message) {
		
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
		
		CompletionStage<ActorRef> resolveOne = receiverProxy.resolveOne(java.time.Duration.ofSeconds(30));
		ActorRef finalReceiver = null;
		try {
			 finalReceiver = resolveOne.toCompletableFuture().get();
		} catch (InterruptedException e) {
			log().error("Error while getting actor");
		} catch (ExecutionException e) {
			log().error("Error while getting actor");
		}
		
		Serialization serialization = SerializationExtension.get(getContext().getSystem());
		byte[] serializedByteArray = serialization.serialize(message.getMessage()).get();
		int serializerId = serialization.findSerializerFor(message.getMessage()).identifier();
		String manifest = Serializers.manifestFor(serialization.findSerializerFor(message.getMessage()), message.getMessage());

		CustomMessage serializedData = new CustomMessage(serializedByteArray, this.sender(), message.getReceiver(),
				serializerId,
				manifest);
		
		final Materializer materializer = ActorMaterializer.create(getContext().getSystem());
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos;
		try {
		
			oos = new ObjectOutputStream(bos);
			oos.writeObject(serializedData);
			oos.flush();
			oos.close();
			bos.close();
		
		} catch (IOException e) {
			log().error("Error while serializing data");
		}
		
		
		
		byte[] byteArrayData = bos.toByteArray();
		ByteString fromArray = ByteString.fromArray(byteArrayData);
		System.out.println(byteArrayData.length);
		
		Source<ByteString,NotUsed> wordSource = Source.single(fromArray);
		Source<ByteString, NotUsed> chunkSource = wordSource.via(new Chunker(1000));
				
		Sink<ByteString, NotUsed> sink =
		  Sink.<ByteString>actorRefWithAck(finalReceiver, "init", "ack", "done", ex ->
		  new StreamFailure(ex));
		 
				
		chunkSource.map(el -> el).runWith(sink, materializer);
		
//		receiverProxy.tell(new BytesMessage<>(message.getMessage(), this.sender(), message.getReceiver()), this.self());
//		receiverProxy.tell(serializedData, this.self());
		
	}

	private void handle(ByteString message) {

		log().info("Am in receiver");
		Serialization serialization = SerializationExtension.get(getContext().getSystem());

		/*
		 * @SuppressWarnings("unused") Object deserializedObject = serialization
		 * .deserialize(message.getByteData(), message.getSerializerID(),
		 * message.getManifest()).get();
		 */		
		getSender().tell("ack", this.getSelf());		
//		message.getReceiverActor().tell(deserializedObject, message.getSenderActor());
	}
	
	private void handle(BytesMessage<?> message) {
		// Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
		message.getReceiver().tell(message.getBytes(), message.getSender());
	}
}
