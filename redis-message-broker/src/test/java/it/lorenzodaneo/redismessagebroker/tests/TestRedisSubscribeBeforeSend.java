package it.lorenzodaneo.redismessagebroker.tests;

import com.lorenzodaneo.RedisSpringBootApplication;
import com.lorenzodaneo.messagebroker.RedisMessageBrokerImpl;
import it.lorenzodaneo.redismessagebroker.tests.mixed.TestConstants;
import it.lorenzodaneo.redismessagebroker.tests.mixed.TestMessage;
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
public class TestRedisSubscribeBeforeSend {

    @Autowired
    private RedisMessageBrokerImpl messageBroker;

    private TestMessage testMessageQueue;
    private Future<TestMessage> futureTestMessageQueue;
    private int receivedCounterQueue = 0;
    private final CompletableFuture<Void> notifierQueue = new CompletableFuture<>();

    private TestMessage testMessageTopic;
    private final List<Future<TestMessage>> futureTestMessagesTopic = new ArrayList<>();
    private int receivedCounterTopic = 0;
    private final CompletableFuture<Void> notifierTopic = new CompletableFuture<>();
    private int groupsLength;


    @BeforeAll
    public void init(){
        testMessageQueue = new TestMessage(randomUUID().toString(), "TestMessageQueue", "A message to testing message broker queue");
        futureTestMessageQueue = Executors.newSingleThreadExecutor().submit(() -> {
            final TestMessage result = new TestMessage();
            messageBroker.subscribeToQueue(TestConstants.CHANNEL_NAME, TestMessage.class, (message) -> messageReceivedQueue(message, result));
            messageBroker.subscribeToQueue(TestConstants.CHANNEL_NAME, TestMessage.class, (message) -> messageReceivedQueue(message, result));
            messageBroker.subscribeToQueue(TestConstants.CHANNEL_NAME, TestMessage.class, (message) -> messageReceivedQueue(message, result));
            notifierQueue.get();
            return result;
        });

        testMessageTopic = new TestMessage(randomUUID().toString(), "TestMessageTopic", "A message to testing message broker topic");
        String[] groups = {"group1", "group2"};
        groupsLength = groups.length;
        for(String group : groups){
            Future<TestMessage> futureTestMessageTopic = Executors.newSingleThreadExecutor().submit(() -> {
                final TestMessage result = new TestMessage();
                messageBroker.subscribeToTopic(TestConstants.CHANNEL_NAME, group, TestMessage.class, (message) -> messageReceivedTopic(message, result));
                messageBroker.subscribeToTopic(TestConstants.CHANNEL_NAME, group, TestMessage.class, (message) -> messageReceivedTopic(message, result));
                notifierTopic.get();
                return result;
            });

            futureTestMessagesTopic.add(futureTestMessageTopic);
        }
    }

    private void messageReceivedQueue(TestMessage srcInstance, TestMessage targetInstance){
        synchronized (notifierQueue){
            receivedCounterQueue++;
            if(receivedCounterQueue == 1){
                targetInstance.setId(srcInstance.getId());
                targetInstance.setType(srcInstance.getType());
                targetInstance.setDescription(srcInstance.getDescription());
                notifierQueue.complete(null);
            }
        }
    }

    private void messageReceivedTopic(TestMessage srcInstance, TestMessage targetInstance){
        synchronized (notifierTopic){
            receivedCounterTopic++;
            if(receivedCounterTopic <= groupsLength){
                targetInstance.setId(srcInstance.getId());
                targetInstance.setType(srcInstance.getType());
                targetInstance.setDescription(srcInstance.getDescription());
                if(receivedCounterTopic == groupsLength)
                    notifierTopic.complete(null);
            }
        }
    }

    @SneakyThrows
    @Test
    @Order(1)
    public void testSendQueueMessage(){
        messageBroker.sendMessageToQueue(TestConstants.CHANNEL_NAME, testMessageQueue);

        TestMessage fromQueue = futureTestMessageQueue.get();
        assert testMessageQueue.getId().equals(fromQueue.getId());
        assert testMessageQueue.getType().equals(fromQueue.getType());
        assert testMessageQueue.getDescription().equals(fromQueue.getDescription());
        assert receivedCounterQueue == 1;
    }


    @SneakyThrows
    @Test
    @Order(2)
    public void testSendTopicMessage(){
        messageBroker.sendMessageToTopic(TestConstants.CHANNEL_NAME, testMessageTopic);

        for(Future<TestMessage> f : futureTestMessagesTopic){
            TestMessage fromTopic = f.get();

            assert testMessageTopic.getId().equals(fromTopic.getId());
            assert testMessageTopic.getType().equals(fromTopic.getType());
            assert testMessageTopic.getDescription().equals(fromTopic.getDescription());
        }
        assert receivedCounterTopic == groupsLength;
    }

}
