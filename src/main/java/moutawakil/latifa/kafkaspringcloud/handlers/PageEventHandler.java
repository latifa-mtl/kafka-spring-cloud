package moutawakil.latifa.kafkaspringcloud.handlers;

import moutawakil.latifa.kafkaspringcloud.events.PageEvent;
import org.springframework.context.annotation.Bean;

import java.util.function.Consumer;

public class PageEventHandler {


    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return (input) -> {
            System.out.println("***************");
            System.out.println(input.toString());
            System.out.println("***************");
        };
    }
}
