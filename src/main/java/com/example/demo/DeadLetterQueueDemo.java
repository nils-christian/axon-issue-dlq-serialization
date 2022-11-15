package com.example.demo;

import static org.awaitility.Awaitility.await;

import java.time.Duration;

import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.config.ConfigurerModule;
import org.axonframework.config.EventProcessingConfiguration;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.MultiStreamableMessageSource;
import org.axonframework.eventhandling.PropagatingErrorHandler;
import org.axonframework.eventhandling.TrackedEventMessage;
import org.axonframework.eventhandling.deadletter.jpa.JpaSequencedDeadLetterQueue;
import org.axonframework.eventhandling.gateway.EventGateway;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.jpa.JpaTokenStore;
import org.axonframework.eventsourcing.eventstore.EmbeddedEventStore;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.StreamableMessageSource;
import org.axonframework.messaging.deadletter.SequencedDeadLetterProcessor;
import org.axonframework.messaging.deadletter.SequencedDeadLetterQueue;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

@SpringBootApplication
public class DeadLetterQueueDemo {

	public static void main( final String[] args ) {
		final ConfigurableApplicationContext applicationContext = SpringApplication.run( DeadLetterQueueDemo.class, args );

		final EventGateway eventGateway = applicationContext.getBean( EventGateway.class );
		eventGateway.publish( 0 );

		final EventProcessingConfiguration eventProcessingConfiguration = applicationContext.getBean( EventProcessingConfiguration.class );
		final SequencedDeadLetterQueue<EventMessage<?>> deadLetterQueue = eventProcessingConfiguration.deadLetterQueue( "P1" ).orElseThrow( );
		await( )
				.atMost( Duration.ofSeconds( 30 ) )
				.until( ( ) -> deadLetterQueue.size( ) == 1 );

		final SequencedDeadLetterProcessor<EventMessage<?>> deadLetterProcessor = eventProcessingConfiguration.sequencedDeadLetterProcessor( "P1" ).orElseThrow( );
		deadLetterProcessor.processAny( );
	}

	@Bean
	public Jackson2ObjectMapperBuilderCustomizer jsonCustomizer( ) {
		return builder -> builder.modulesToInstall( new ParameterNamesModule( JsonCreator.Mode.DELEGATING ) );
	}

	@Bean
	public TokenStore tokenStore( final EntityManagerProvider entityManagerProvider ) {
		return JpaTokenStore
				.builder( )
				.entityManagerProvider( entityManagerProvider )
				.serializer( JacksonSerializer.defaultSerializer( ) )
				.build( );
	}

	@Bean
	ConfigurerModule eventProcessingConfigurerModule( final EntityManagerProvider entityManagerProvider, final Serializer serializer ) {
		return c -> c
				.eventProcessing( )
				.configureDefaultStreamableMessageSource( conf -> MultiStreamableMessageSource
						.builder( )
						.addMessageSource( "A", ( StreamableMessageSource<TrackedEventMessage<?>> ) conf.eventBus( ) )
						.addMessageSource( "B", EmbeddedEventStore
								.builder( )
								.storageEngine( new InMemoryEventStorageEngine( ) )
								.build( ) )
						.build( ) )
				.registerDefaultListenerInvocationErrorHandler( conf -> PropagatingErrorHandler.INSTANCE )
				.registerDeadLetterQueue( "P1", conf -> {
					final EventProcessingConfiguration eventProcessingConfiguration = conf.eventProcessingConfiguration( );
					return JpaSequencedDeadLetterQueue
							.builder( )
							.processingGroup( "P1" )
							.transactionManager( eventProcessingConfiguration.transactionManager( "P1" ) )
							.entityManagerProvider( entityManagerProvider )
							.serializer( serializer )
							.build( );
				} );
	}

	@Component
	@ProcessingGroup( "P1" )
	public class MyEventHandler {

		@EventHandler
		public void on( final Object o ) {
			throw new IllegalStateException( "I cannot handle this event" );
		}

	}

}