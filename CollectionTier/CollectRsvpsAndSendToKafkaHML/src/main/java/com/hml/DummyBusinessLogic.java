package com.hml;

import java.util.logging.Logger;
import org.springframework.stereotype.Component;

@Component
public class DummyBusinessLogic {

    private static final Logger logger = 
            Logger.getLogger(DummyBusinessLogic.class.getName());

    public RsvpDocument dummyLogic(RsvpDocument rsvp) {
        logger.info("Dummy business logic ...");

        return rsvp;
    }

}
