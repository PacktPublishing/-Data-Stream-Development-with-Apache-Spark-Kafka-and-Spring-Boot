package com.rsvps;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

@Component
class RsvpsWebSocketHandler extends AbstractWebSocketHandler {

    @Override
    public void handleMessage(WebSocketSession session,
            WebSocketMessage<?> message) {
        System.out.println(message.getPayload());     
    }
}
