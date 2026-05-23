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
const app_controller_1 = __webpack_require__(7);
const app_service_1 = __webpack_require__(8);
let AppModule = class AppModule {
};
exports.AppModule = AppModule;
exports.AppModule = AppModule = tslib_1.__decorate([
    (0, common_1.Module)({
        imports: [],
        controllers: [app_controller_1.AppController],
        providers: [app_service_1.AppService],
    })
], AppModule);


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


Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.AppController = void 0;
const tslib_1 = __webpack_require__(5);
const common_1 = __webpack_require__(6);
const microservices_1 = __webpack_require__(2);
let AppController = class AppController {
    handleUserCreated(data) {
        console.log('Event received: user_created', data);
    }
};
exports.AppController = AppController;
tslib_1.__decorate([
    (0, microservices_1.EventPattern)('user_created'),
    tslib_1.__param(0, (0, microservices_1.Payload)()),
    tslib_1.__metadata("design:type", Function),
    tslib_1.__metadata("design:paramtypes", [Object]),
    tslib_1.__metadata("design:returntype", void 0)
], AppController.prototype, "handleUserCreated", null);
exports.AppController = AppController = tslib_1.__decorate([
    (0, common_1.Controller)()
], AppController);


/***/ }),
/* 8 */
/***/ ((__unused_webpack_module, exports, __webpack_require__) => {


Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.AppService = void 0;
const tslib_1 = __webpack_require__(5);
const common_1 = __webpack_require__(6);
let AppService = class AppService {
    getData() {
        return { message: 'Hello API' };
    }
};
exports.AppService = AppService;
exports.AppService = AppService = tslib_1.__decorate([
    (0, common_1.Injectable)()
], AppService);


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
async function ensureKafkaTopics() {
    const kafka = new kafkajs_1.Kafka({
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