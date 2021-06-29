package cn.com.dmg.rabbitmqlearn.workqueues.handack;

import cn.com.dmg.rabbitmqlearn.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

public class HandAckWork01 {
    private static final String ACK_QUEUE_NAME="ack_queue";
    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        System.out.println("C1  等待接收消息处理时间较短");
        // 消息消费的时候如何处理消息
        DeliverCallback deliverCallback=(consumerTag, delivery)->{
            String message= new String(delivery.getBody());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(" 接收到消息:"+message);
            /**
            * 1. 消息标记 tag
            * 2. 是否批量应答未应答消息
            */
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(),false);
        };
        //设置为不公平分发
        channel.basicQos(1);
        // 采用手动应答
        channel.basicConsume(ACK_QUEUE_NAME,false,deliverCallback,(consumerTag)->{
            System.out.println(consumerTag+" 消费者取消消费接口回调逻辑");
        });
    }
}
