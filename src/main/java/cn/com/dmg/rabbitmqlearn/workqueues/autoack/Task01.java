package cn.com.dmg.rabbitmqlearn.workqueues.autoack;

import cn.com.dmg.rabbitmqlearn.utils.RabbitMqUtils;
import com.rabbitmq.client.Channel;

import java.util.Scanner;

/**
 * @ClassName Task01 生产者
 * @Description
 * @author zhum
 * @date 2021/6/22 11:22
 */
public class Task01 {
private static final String QUEUE_NAME="hello";
    public static void main(String[] args) throws Exception {
            Channel channel= RabbitMqUtils.getChannel();
            channel.queueDeclare(QUEUE_NAME,false,false,false,null);
            // 从控制台当中接受信息
            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()){
                String message = scanner.next();
                channel.basicPublish("",QUEUE_NAME,null,message.getBytes());
                System.out.println(" 发送消息完成:"+message);
            }
    }
}
