import { Controller, Get, Inject, OnModuleInit} from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';

@Controller()
export class AppController implements OnModuleInit {
    constructor(
      @Inject('KAFKA_SERVICE') private readonly kafkaClient: ClientKafka,
    ) {}
    async onModuleInit() {
        this.kafkaClient.connect();
    }

    @Get('create-user')
    async createUser() {
        const user = {
          id: 1,
          name: 'John Doe',
          email: 'john.doe@example.com',
          password: 'password',
        }
       this.kafkaClient.emit('user_created', user);
        return { 
          message: 'User created and event sent to Kafka',
          user: user,
        };
    }
}