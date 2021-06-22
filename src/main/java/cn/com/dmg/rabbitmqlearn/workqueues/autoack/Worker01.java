package cn.com.dmg.rabbitmqlearn.workqueues.autoack;

import cn.com.dmg.rabbitmqlearn.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;

/**
 * @ClassName Worker01 消费者 自动应答
 * @Description
 * @author zhum
 * @date 2021/6/22 11:19
 */
public class Worker01 {
    private static final String QUEUE_NAME="hello";
    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        System.out.println("C2  消费者启动等待消费......");
        channel.basicConsume(QUEUE_NAME,
                true,
                (consumerTag,delivery)->{
                    String receivedMessage = new String(delivery.getBody());
                    System.out.println(" 接收到消息:"+receivedMessage);
                },
                (consumerTag)->{
                    System.out.println(consumerTag+" 消费者取消消费接口回调逻辑");
                });
    }
}
