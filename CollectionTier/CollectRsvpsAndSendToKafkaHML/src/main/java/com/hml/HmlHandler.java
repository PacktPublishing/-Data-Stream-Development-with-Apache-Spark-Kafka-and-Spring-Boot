package com.hml;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

@Component
public class HmlHandler {

    private static final Logger logger = Logger.getLogger(HmlHandler.class.getName());

    private static final String COLLECTION_NAME = "temprsvps";
    private static final long DELAY_MS = 100L;
    private static final long RETRIES = 2L;
    private static final long THRESHOLD = 1000L;

    private final RsvpsKafkaProducer rsvpsKafkaProducer;
    private final ReactiveMongoTemplate mongoTemplate;
    private final DummyBusinessLogic dummyBusinessLogic;

    public HmlHandler(RsvpsKafkaProducer rsvpsKafkaProducer,
            ReactiveMongoTemplate mongoTemplate,
            DummyBusinessLogic dummyBusinessLogic) {
        this.rsvpsKafkaProducer = rsvpsKafkaProducer;
        this.mongoTemplate = mongoTemplate;
        this.dummyBusinessLogic = dummyBusinessLogic;
    }    

    public void recoverAfterRestart() {
        mongoTemplate.count(new Query(), COLLECTION_NAME)
                .subscribe(count -> {
                    if (count > 0) {
                        logger.info("Recover after restart ...");

                        recoverRsvps();
                    }
                });
    }

    // RBML
    public void handleNewRsvp(String message) {
        RsvpDocument rsvpDocument
                = new RsvpDocument(
                        Instant.now().toEpochMilli(), message, RsvpStatus.PENDING);
                               
        insertRsvps(Collections.singletonList(rsvpDocument));
    }

    private void insertRsvps(List<RsvpDocument> rsvpDocumentList) {
        Flux<RsvpDocument> fluxInsertRsvp = 
            mongoTemplate.insert(rsvpDocumentList, RsvpDocument.class);
        processRsvp(fluxInsertRsvp);
    }

    // SBML
    private void recoverRsvps() {
        Flux<RsvpDocument> fluxFindAllRsvps = 
                mongoTemplate.findAll(RsvpDocument.class);
        processRsvp(fluxFindAllRsvps);
    }
    
    private void removeRsvp(RsvpDocument rsvp) {
        mongoTemplate.remove(rsvp, COLLECTION_NAME)
                .retry(RETRIES)
                .subscribe();
    }

    private void updateRsvp(RsvpDocument rsvp) {
        mongoTemplate.updateFirst(
                new Query().addCriteria(Criteria.where("id").is(rsvp.getId())),
                new Update().set("status", RsvpStatus.FAILED), RsvpDocument.class)
                .retry(RETRIES)
                .subscribe();
    }

    @Scheduled(
            initialDelayString = "${initialDelay.in.milliseconds}", 
            fixedRateString = "${fixedRate.in.milliseconds}"
    )
    private void internalRecoverOfFailures() {
        
        logger.info("Scheduled job for failed RSVP ...");
        
        mongoTemplate.count(new Query().addCriteria(
                Criteria.where("status").is(RsvpStatus.FAILED)), COLLECTION_NAME)
                .subscribe(count -> {
                    logger.log(Level.INFO, "Failures found during scheduled job: {0}", count);
                    
                    if (count > THRESHOLD) {
                        logger.info("Failures threshold exceeded ...");
                        
                        // Do one or all:
                        //  - stop ingesting RSVPs
                        //  - send a notification                        
                        //  - replay failures
                        //  - ...
                    } else if (count > 0) {
                        logger.info("Replay failures ...");

                        recoverFailedRsvps();
                    }
                });
    }
    
    private void recoverFailedRsvps() {
        Flux<RsvpDocument> fluxFindAllFailedRsvps = 
                mongoTemplate.find(new Query().addCriteria(
                Criteria.where("status").is(RsvpStatus.FAILED)),
                RsvpDocument.class, COLLECTION_NAME);
        
        processRsvp(fluxFindAllFailedRsvps);
    }

    // common helper for RBML and SBML
    private void processRsvp(Flux<RsvpDocument> rsvpDocument) {

        rsvpDocument.map(dummyBusinessLogic::dummyLogic)
                .delayElements(Duration.ofMillis(DELAY_MS))
                .doOnNext(rsvpsKafkaProducer::sendRsvpMessage)
                .subscribe(t -> {
                    if (t.getStatus().equals(RsvpStatus.PROCESSED)) {
                        logger.info(() -> "Deleting RSVP with id: " + t.getId());
                        removeRsvp(t);

                    } else if (t.getStatus().equals(RsvpStatus.UNSENT)) {
                        logger.info(() -> "Mark as 'failed' RSVP with id: " + t.getId());
                        t.setStatus(RsvpStatus.FAILED);
                        updateRsvp(t);
                    }
                });
    }
}
