import { Controller, Inject, OnModuleInit } from "@nestjs/common";
import { ClientKafka, Payload, EventPattern } from "@nestjs/microservices";

@Controller()
export class AppController implements OnModuleInit {
  constructor(
    @Inject('KAFKA_SERVICE')
    private readonly kafkaClient: ClientKafka
  ) { }

  async onModuleInit() {
    await this.kafkaClient.connect();
  }

  @EventPattern('user_created')
  async handleUserCreated(@Payload() data: any) {
    try {
      console.log('Main event recieved!');
      console.log(data);
      throw new Error('Payment Service Failed!')
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error)
      console.log('Sending event to DLQ', errorMessage);
      this.kafkaClient.emit(
        'user_created_dlq',
        {
          failedData: data,
          error: errorMessage,
          failedAt: new Date(),
        }
      )
    }
  }

  @EventPattern('user_created_dlq')
  handleDLQ(@Payload() data: any) {
    console.log("Recieved dead letter message")
    console.log(data)
  }
}