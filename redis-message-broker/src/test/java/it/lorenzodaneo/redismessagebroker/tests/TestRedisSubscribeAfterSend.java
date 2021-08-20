package it.lorenzodaneo.redismessagebroker.tests;

import com.lorenzodaneo.RedisSpringBootApplication;
import com.lorenzodaneo.messagebroker.RedisMessageBrokerImpl;
import it.lorenzodaneo.redismessagebroker.tests.mixed.TestConstants;
import it.lorenzodaneo.redismessagebroker.tests.mixed.TestMessage;
import lombok.Data;
import lombok.SneakyThrows;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.UUID.randomUUID;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        classes = RedisSpringBootApplication.class
)
public class TestRedisSubscribeAfterSend {

    @Autowired
    private RedisMessageBrokerImpl messageBroker;

    private final List<TestMessage> testMessagesQueue = new ArrayList<>();
    private final CompletableFuture<Void> notifierQueue = new CompletableFuture<>();
    private int receivedCounterQueue = 0;

    private final List<TestMessage> testMessagesTopic = new ArrayList<>();
    private final TestTopicGroup[] groupsTopic = new TestTopicGroup[]{new TestTopicGroup("group1"), new TestTopicGroup("group2")};


    @BeforeAll
    public void init(){
        String idQueueAggregate = randomUUID().toString();
        testMessagesQueue.add(new TestMessage(idQueueAggregate, "TestMessageQueue1", "A message to testing message broker queue"));
        testMessagesQueue.add(new TestMessage(idQueueAggregate, "TestMessageQueue2", "A message to testing message broker queue"));
        String idTopicAggregate = randomUUID().toString();
        testMessagesTopic.add(new TestMessage(idTopicAggregate, "TestMessageTopic1", "A message to testing message broker topic"));
        testMessagesTopic.add(new TestMessage(idTopicAggregate, "TestMessageTopic2", "A message to testing message broker topic"));

        for(TestMessage mQueue : testMessagesQueue)
            messageBroker.sendMessageToQueue(TestConstants.CHANNEL_NAME, mQueue);
        for(TestMessage mTopic : testMessagesTopic)
            messageBroker.sendMessageToTopic(TestConstants.CHANNEL_NAME, mTopic);
    }

    @SneakyThrows
    @Test
    @Order(1)
    public void testReceiveQueueMessage(){
        Future<List<TestMessage>> futureTestMessageQueue = Executors.newSingleThreadExecutor().submit(() -> {
            final List<TestMessage> result = new ArrayList<>();
            messageBroker.subscribeToQueue(TestConstants.CHANNEL_NAME, TestMessage.class, (message) -> messageReceivedQueue(message, result));
            messageBroker.subscribeToQueue(TestConstants.CHANNEL_NAME, TestMessage.class, (message) -> messageReceivedQueue(message, result));
            messageBroker.subscribeToQueue(TestConstants.CHANNEL_NAME, TestMessage.class, (message) -> messageReceivedQueue(message, result));

            final List<Future<Boolean>> awaiters = new ArrayList<>();
            for(int i = 0; i < testMessagesQueue.size(); i++){
                 awaiters.add(Executors.newSingleThreadExecutor().submit(() -> {
                    notifierQueue.get();
                    return true;
                }));
            }
            for(Future<Boolean> awaiter : awaiters)
                awaiter.get();
            return result;
        });

        List<TestMessage> testMessages = futureTestMessageQueue.get();
        assert receivedCounterQueue == testMessages.size();

        for(int i = 0; i < testMessages.size(); i++){
            TestMessage fromQueue = testMessages.get(i);
            TestMessage find = testMessagesQueue.get(i);

            assert find.getId().equals(fromQueue.getId());
            assert find.getType().equals(fromQueue.getType());
            assert find.getDescription().equals(fromQueue.getDescription());
        }
    }

    private void messageReceivedQueue(TestMessage srcInstance, List<TestMessage> targetList){
        synchronized (notifierQueue){
            receivedCounterQueue++;
            targetList.add(srcInstance);
            notifierQueue.complete(null);
        }
    }


    @SneakyThrows
    @Test
    @Order(2)
    public void testReceiveTopicMessage(){
        for(TestTopicGroup group : groupsTopic){
            Future<List<TestMessage>> futureTestMessageTopic = Executors.newSingleThreadExecutor().submit(() -> {
                final List<TestMessage> result = new ArrayList<>();
                messageBroker.subscribeToTopic(TestConstants.CHANNEL_NAME, group.getName(), TestMessage.class, (message) -> messageReceivedTopic(group, message, result));
                messageBroker.subscribeToTopic(TestConstants.CHANNEL_NAME, group.getName(), TestMessage.class, (message) -> messageReceivedTopic(group, message, result));

                final List<Future<Boolean>> awaiters = new ArrayList<>();
                for(int i = 0; i < testMessagesTopic.size(); i++){
                    awaiters.add(Executors.newSingleThreadExecutor().submit(() -> {
                        group.getNotifier().get();
                        return true;
                    }));
                }
                for(Future<Boolean> awaiter : awaiters)
                    awaiter.get();
                return result;
            });

            List<TestMessage> testMessages = futureTestMessageTopic.get();
            assert group.getReceivedCounter() == testMessages.size();

            for(int i = 0; i < testMessages.size(); i++){
                TestMessage fromTopic = testMessages.get(i);
                TestMessage find = testMessagesTopic.get(i);

                assert find.getId().equals(fromTopic.getId());
                assert find.getType().equals(fromTopic.getType());
                assert find.getDescription().equals(fromTopic.getDescription());
            }
        }
    }

    private void messageReceivedTopic(TestTopicGroup group, TestMessage srcInstance, List<TestMessage> targetList){
        synchronized (group.getNotifier()){
            group.setReceivedCounter(group.getReceivedCounter() + 1);
            targetList.add(srcInstance);
            group.getNotifier().complete(null);
        }
    }

    @Data
    private static class TestTopicGroup{
        private final String name;
        private final CompletableFuture<Void> notifier = new CompletableFuture<>();
        private int receivedCounter = 0;
    }

}
