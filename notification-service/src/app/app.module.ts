import { Module } from "@nestjs/common";
import { ClientsModule, Transport } from "@nestjs/microservices";
import { AppController } from "./app.controller";

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_SERVICE',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'notification-client',
            brokers: ['localhost:9092'],
          },
          consumer: {
            groupId: 'notification-consumer-group'
          }
        }
      }
    ])
  ],
  controllers: [AppController]
})

export class AppModule { };
