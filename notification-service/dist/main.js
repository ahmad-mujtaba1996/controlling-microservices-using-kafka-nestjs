/******/ (() => { // webpackBootstrap
/******/ 	"use strict";
/******/ 	var __webpack_modules__ = ([
/* 0 */,
/* 1 */
/***/ ((module) => {

module.exports = require("@nestjs/core");

/***/ }),
/* 2 */
/***/ ((module) => {

module.exports = require("@nestjs/microservices");

/***/ }),
/* 3 */
/***/ ((module) => {

module.exports = require("kafkajs");

/***/ }),
/* 4 */
/***/ ((__unused_webpack_module, exports, __webpack_require__) => {


Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.AppModule = void 0;
const tslib_1 = __webpack_require__(5);
const common_1 = __webpack_require__(6);
const microservices_1 = __webpack_require__(2);
const app_controller_1 = __webpack_require__(7);
let AppModule = class AppModule {
};
exports.AppModule = AppModule;
exports.AppModule = AppModule = tslib_1.__decorate([
    (0, common_1.Module)({
        imports: [
            microservices_1.ClientsModule.register([
                {
                    name: 'KAFKA_SERVICE',
                    transport: microservices_1.Transport.KAFKA,
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
        controllers: [app_controller_1.AppController]
    })
], AppModule);
;


/***/ }),
/* 5 */
/***/ ((module) => {

module.exports = require("tslib");

/***/ }),
/* 6 */
/***/ ((module) => {

module.exports = require("@nestjs/common");

/***/ }),
/* 7 */
/***/ ((__unused_webpack_module, exports, __webpack_require__) => {


var _a;
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.AppController = void 0;
const tslib_1 = __webpack_require__(5);
const common_1 = __webpack_require__(6);
const microservices_1 = __webpack_require__(2);
let AppController = class AppController {
    constructor(kafkaClient) {
        this.kafkaClient = kafkaClient;
    }
    async onModuleInit() {
        await this.kafkaClient.connect();
    }
    async handleUserCreated(data) {
        try {
            console.log('Main event recieved!');
            console.log(data);
            throw new Error('Payment Service Failed!');
        }
        catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            console.log('Sending event to DLQ', errorMessage);
            this.kafkaClient.emit('user_created_dlq', {
                failedData: data,
                error: errorMessage,
                failedAt: new Date(),
            });
        }
    }
    handleDLQ(data) {
        console.log("Recieved dead letter message");
        console.log(data);
    }
};
exports.AppController = AppController;
tslib_1.__decorate([
    (0, microservices_1.EventPattern)('user_created'),
    tslib_1.__param(0, (0, microservices_1.Payload)()),
    tslib_1.__metadata("design:type", Function),
    tslib_1.__metadata("design:paramtypes", [Object]),
    tslib_1.__metadata("design:returntype", Promise)
], AppController.prototype, "handleUserCreated", null);
tslib_1.__decorate([
    (0, microservices_1.EventPattern)('user_created_dlq'),
    tslib_1.__param(0, (0, microservices_1.Payload)()),
    tslib_1.__metadata("design:type", Function),
    tslib_1.__metadata("design:paramtypes", [Object]),
    tslib_1.__metadata("design:returntype", void 0)
], AppController.prototype, "handleDLQ", null);
exports.AppController = AppController = tslib_1.__decorate([
    (0, common_1.Controller)(),
    tslib_1.__param(0, (0, common_1.Inject)('KAFKA_SERVICE')),
    tslib_1.__metadata("design:paramtypes", [typeof (_a = typeof microservices_1.ClientKafka !== "undefined" && microservices_1.ClientKafka) === "function" ? _a : Object])
], AppController);


/***/ })
/******/ 	]);
/************************************************************************/
/******/ 	// The module cache
/******/ 	var __webpack_module_cache__ = {};
/******/ 	
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/ 		// Check if module is in cache
/******/ 		var cachedModule = __webpack_module_cache__[moduleId];
/******/ 		if (cachedModule !== undefined) {
/******/ 			return cachedModule.exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = __webpack_module_cache__[moduleId] = {
/******/ 			// no module.id needed
/******/ 			// no module.loaded needed
/******/ 			exports: {}
/******/ 		};
/******/ 	
/******/ 		// Execute the module function
/******/ 		__webpack_modules__[moduleId](module, module.exports, __webpack_require__);
/******/ 	
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/ 	
/************************************************************************/
var __webpack_exports__ = {};
// This entry needs to be wrapped in an IIFE because it needs to be isolated against other modules in the chunk.
(() => {
var exports = __webpack_exports__;

Object.defineProperty(exports, "__esModule", ({ value: true }));
const core_1 = __webpack_require__(1);
const microservices_1 = __webpack_require__(2);
const kafkajs_1 = __webpack_require__(3);
const app_module_1 = __webpack_require__(4);
const KAFKA_BROKERS = ['localhost:9092'];
const USER_CREATED_TOPIC = 'user_created';
const USER_CREATED_DLQ_TOPIC = 'user_created_dlq';
async function ensureKafkaTopics() {
    const kafka = new kafkajs_1.Kafka({
        clientId: 'notification-admin',
        brokers: KAFKA_BROKERS,
    });
    const admin = kafka.admin();
    await admin.connect();
    try {
        const existingTopics = await admin.listTopics();
        const topicsToCreate = [];
        if (!existingTopics.includes(USER_CREATED_TOPIC)) {
            topicsToCreate.push({
                topic: USER_CREATED_TOPIC,
                numPartitions: 1,
                replicationFactor: 1,
            });
        }
        if (!existingTopics.includes(USER_CREATED_DLQ_TOPIC)) {
            topicsToCreate.push({
                topic: USER_CREATED_DLQ_TOPIC,
                numPartitions: 1,
                replicationFactor: 1,
            });
        }
        if (topicsToCreate.length > 0) {
            await admin.createTopics({
                topics: topicsToCreate,
                waitForLeaders: true,
            });
        }
    }
    finally {
        await admin.disconnect();
    }
}
// Consumer Side
async function bootstrap() {
    await ensureKafkaTopics();
    const app = await core_1.NestFactory.createMicroservice(app_module_1.AppModule, {
        transport: microservices_1.Transport.KAFKA,
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
    });
    await app.listen();
    console.log('Notification consumer is running');
}
bootstrap();

})();

/******/ })()
;
//# sourceMappingURL=main.js.map