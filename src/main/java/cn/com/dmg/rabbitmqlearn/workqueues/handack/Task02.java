package cn.com.dmg.rabbitmqlearn.workqueues.handack;

import cn.com.dmg.rabbitmqlearn.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;

import java.util.Scanner;

/**
 * @ClassName Task02
 * @Description 最终实现的效果为：
 *  生产者发送:aa bb cc dd
 *  正常来讲是
 *  work1 aa cc
 *  work2 bb dd
 *  但是work2处理消息慢，当work2还未处理完的时候，关闭work2，那么
 *  bb 和 dd两条消息会被重新放回队列中，被work1处理，消息没有丢失
 * @author zhum
 * @date 2021/6/22 11:29
 */
public class Task02 {
    private static final String TASK_QUEUE_NAME = "ack_queue";
    public static void main(String[] argv) throws Exception {
        Channel channel = RabbitMqUtils.getChannel();
        channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
        Scanner sc = new Scanner(System.in);
        System.out.println(" 请输入信息");
        while (sc.hasNext()) {
            String message = sc.nextLine();
            channel.basicPublish("", TASK_QUEUE_NAME, null, message.getBytes("UTF-8"));
            System.out.println(" 生产者发出消息" + message);
        }
    }
}
