import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { Kafka } from 'kafkajs';
import { AppModule } from './app/app.module';

const KAFKA_BROKERS = ['localhost:9092'];
const USER_CREATED_TOPIC = 'user_created';

async function ensureKafkaTopics() {
  const kafka = new Kafka({
    clientId: 'notification-admin',
    brokers: KAFKA_BROKERS,
  });
  const admin = kafka.admin();

  await admin.connect();
  try {
    const existingTopics = await admin.listTopics();
    if (!existingTopics.includes(USER_CREATED_TOPIC)) {
      await admin.createTopics({
        topics: [
          {
            topic: USER_CREATED_TOPIC,
            numPartitions: 1,
            replicationFactor: 1,
          },
        ],
        waitForLeaders: true,
      });
    }
  } finally {
    await admin.disconnect();
  }
}

// Consumer Side
async function bootstrap() {
  await ensureKafkaTopics();

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    {
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: 'notification-consumer',
          brokers: KAFKA_BROKERS,
          retry: {
            retries: 10,
            initialRetryTime: 300,
          },
        },
        consumer: {
          groupId: 'notification-group',
        },
        subscribe: {
          fromBeginning: true,
          allowAutoTopicCreation: true,
        },
      },
    },
  );
  await app.listen();
  console.log('Notification consumer is running');
}


bootstrap();