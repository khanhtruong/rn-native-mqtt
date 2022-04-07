"use strict";
import { NativeEventEmitter, NativeModules} from 'react-native';
import { stringToBytes } from 'convert-string';
Object.defineProperty(exports, "__esModule", { value: true });
const tiny_emitter_1 = require("tiny-emitter");
const randomId = require("random-id");
const buffer_1 = require("buffer");
const { NativeMqtt } = NativeModules;
const mqttEventEmitter = new NativeEventEmitter(NativeMqtt);



var Event;
(function (Event) {
    Event["Connect"] = "connect";
    Event["Disconnect"] = "disconnect";
    Event["Message"] = "message";
    Event["Error"] = "error";
})(Event = exports.Event || (exports.Event = {}));
class Client {
    constructor(url) {
        this.connected = false;
        this.closed = false;
        this.emitter = new tiny_emitter_1.TinyEmitter();
        this.id = randomId(12);
        this.url = url;
        NativeMqtt.newClient(this.id);
        mqttEventEmitter.addListener('rn-native-mqtt_connect', (event) => {
            if (event.id !== this.id) {
                return;
            }
            this.connected = true;
            this.emitter.emit(Event.Connect, event.reconnect);
        });
        mqttEventEmitter.addListener('rn-native-mqtt_message', (event) => {
            if (event.id !== this.id) {
                return;
            }
            this.emitter.emit(Event.Message, event.topic, buffer_1.Buffer.from(event.message, 'base64'));
        });
        mqttEventEmitter.addListener('rn-native-mqtt_disconnect', (event) => {
            if (event.id !== this.id) {
                return;
            }
            this.connected = false;
            this.emitter.emit(Event.Disconnect, event.cause);
        });
        mqttEventEmitter.addListener('rn-native-mqtt_error', (event) => {
            if (event.id !== this.id) {
                return;
            }
            this.emitter.emit(Event.Error, event.error);
        });
    }
    connect(options) {
        return new Promise((resolve, reject) => {
            if (this.closed) {
                reject(new Error('client already closed'));
            }
            if (this.connected) {
                reject(new Error('client already connected'));
            }
            const opts = Object.assign({}, options);
            if (opts.tls && opts.tls.p12) {
                opts.tls = Object.assign({}, opts.tls);
                opts.tls.p12 = opts.tls.p12.toString('base64');
            }
            NativeMqtt.connect(this.id, this.url, opts, (err) => {
                if (err) {
                    reject(new Error(err));
                    return;
                }
                console.log('Connected to NativeMqtt!');
                this.connected = true;
                resolve();
            });
        });
    }
    subscribe(topics, qos) {
        if (this.closed) {
            throw new Error('client already closed');
        }
        if (!this.connected) {
            throw new Error('client not connected');
        }
        NativeMqtt.subscribe(this.id, topics, qos);
    }
    unsubscribe(topics) {
        if (this.closed) {
            throw new Error('client already closed');
        }
        if (!this.connected) {
            throw new Error('client not connected');
        }
        NativeMqtt.unsubscribe(this.id, topics);
    }
    publish(topic, message, qos = 0, retained = false) {
        if (this.closed) {
            throw new Error('client already closed');
        }
        if (!this.connected) {
            throw new Error('client not connected');
        }
        
        var hex = false;
        var regexp = /^[0-9a-fA-F]+$/;
        if (regexp.test(message))
          {
            hex = true;
          }
        else
          {
            hex = false;
          }
        var result = [];
        if(hex) {
            while (message.length >= 2) { 
            result.push(parseInt(message.substring(0, 2), 16));
            message = message.substring(2, message.length);
            }
        }
      NativeMqtt.publish(this.id, topic, hex ? result : typeof message !== String ? message: stringToBytes(message) , qos, retained);  
    }
    disconnect() {
        if (this.closed) {
            throw new Error('client already closed');
        }
        NativeMqtt.disconnect(this.id);
    }
    close() {
        if (this.connected) {
            throw new Error('client not disconnected');
        }
        NativeMqtt.close(this.id);
        this.closed = true;
        this.emitter = null;
    }
    on(name, handler, context) {
        if (this.closed) {
            throw new Error('client already closed');
        }
        this.emitter.on(name, handler, context);
    }
    once(name, handler, context) {
        if (this.closed) {
            throw new Error('client already closed');
        }
        this.emitter.once(name, handler, context);
    }
    off(name, handler) {
        if (this.closed) {
            throw new Error('client already closed');
        }
        this.emitter.off(name, handler);
    }
}
exports.Client = Client;
//# sourceMappingURL=index.js.map
