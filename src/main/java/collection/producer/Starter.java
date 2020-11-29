package collection.producer;


public class Starter {

    public static void main(String[] args) throws InterruptedException {
        KafkaPublisher publisher = new KafkaPublisher();
        Collector collector = new Collector(publisher);
        scheduleCollect(collector);
    }

    private static void scheduleCollect(Collector collector) throws InterruptedException {
        //repeat 2 times all api call if first fails
        ScheduledExecutorRepeat executorRepeat = new ScheduledExecutorRepeat(collector, 2);
        executorRepeat.repeat();
    }
}
