import { NativeModules, NativeEventEmitter } from "react-native"
import { TinyEmitter } from "tiny-emitter"
import randomId from "random-id"
import { Buffer } from "buffer"
import { Will } from "./Will"
import { stringToBytes } from "convert-string"

const { NativeMqtt } = NativeModules
const mqttEventEmitter = new NativeEventEmitter(NativeMqtt)

export interface TlsOptions {
  caDer?: String
  cert?: string
  key?: string
  p12?: string
  pass?: string
}

export interface ConnectionOptions {
  clientId: string
  cleanSession?: boolean
  keepAlive?: number
  timeout?: number
  maxInFlightMessages?: number
  autoReconnect?: boolean
  username?: string
  password?: string
  tls?: TlsOptions
  allowUntrustedCA?: boolean
  enableSsl?: boolean
  will?: Will
}

export interface PublishOptions {
  retained?: boolean
  qos?: number
}

export enum Event {
  Connect = "connect",
  Disconnect = "disconnect",
  Message = "message",
  Error = "error",
}

export type ConnectEventHandler = (reconnect: boolean) => void
export type MessageEventHandler = (topic: string, message: Buffer) => void
export type DisconnectEventHandler = (cause: string) => void
export type ErrorEventHandler = (error: string) => void

export class Client {
  private id: string
  private emitter: TinyEmitter

  private url: string
  private connected: boolean = false
  private closed: boolean = false

  constructor(url: string) {
    this.emitter = new TinyEmitter()
    this.id = randomId(12)
    this.url = url

    NativeMqtt.newClient(this.id)

    mqttEventEmitter.addListener(
      "rn-native-mqtt_connect",
      (event: { id: string; reconnect: boolean }) => {
        if (event.id !== this.id) {
          return
        }

        this.connected = true
        this.emitter.emit(Event.Connect, event.reconnect)
      }
    )

    mqttEventEmitter.addListener(
      "rn-native-mqtt_message",
      (event: { id: string; topic: string; message: string }) => {
        if (event.id !== this.id) {
          return
        }

        this.emitter.emit(
          Event.Message,
          event.topic,
          Buffer.from(event.message, "base64")
        )
      }
    )

    mqttEventEmitter.addListener(
      "rn-native-mqtt_disconnect",
      (event: { id: string; cause: string }) => {
        if (event.id !== this.id) {
          return
        }

        this.connected = false
        this.emitter.emit(Event.Disconnect, event.cause)
      }
    )

    mqttEventEmitter.addListener(
      "rn-native-mqtt_error",
      (event: { id: string; error: string }) => {
        if (event.id !== this.id) {
          return
        }

        this.emitter.emit(Event.Error, event.error)
      }
    )
  }

  public connect(options: ConnectionOptions): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.closed) {
        reject(new Error("client already closed"))
      }
      if (this.connected) {
        reject(new Error("client already connected"))
      }
      const opts = Object.assign({}, options)
      NativeMqtt.connect(this.id, this.url, opts, (err) => {
        if (err) {
          reject(new Error(err))
          return
        }
        console.log("Connected to NativeMqtt!")
        this.connected = true
        resolve()
      })
    })
  }

  public subscribe(topics: string[], qos: number[]) {
    if (this.closed) {
      throw new Error("client already closed")
    }

    if (!this.connected) {
      throw new Error("client not connected")
    }

    NativeMqtt.subscribe(this.id, topics, qos)
  }

  public unsubscribe(topics: string[]) {
    if (this.closed) {
      throw new Error("client already closed")
    }

    if (!this.connected) {
      throw new Error("client not connected")
    }

    NativeMqtt.unsubscribe(this.id, topics)
  }

  public publish(
    topic: string,
    message: string,
    bytesArray: number[] | undefined = undefined,
    qos: number = 0,
    retained: boolean = false
  ) {
    if (this.closed) {
      throw new Error("client already closed")
    }

    if (!this.connected) {
      throw new Error("client not connected")
    }

    if (bytesArray) {
      NativeMqtt.publish(this.id, topic, bytesArray, qos, retained)
      return
    }

    NativeMqtt.publish(this.id, topic, stringToBytes(message), qos, retained)
  }

  public willmessage(
    topic: string,
    message: Buffer,
    message2: number[] | undefined = undefined,
    qos: number = 0,
    retained: boolean = false
  ) {
    if (this.closed) {
      throw new Error("client already closed")
    }

    if (message2) {
      NativeMqtt.publish(this.id, topic, message2, qos, retained)
      return
    }

    NativeMqtt.willmessage(
      this.id,
      topic,
      stringToBytes(message),
      qos,
      retained
    )
  }

  public disconnect() {
    if (this.closed) {
      throw new Error("client already closed")
    }

    NativeMqtt.disconnect(this.id)
  }

  public close() {
    if (this.connected) {
      throw new Error("client not disconnected")
    }

    NativeMqtt.close(this.id)
    this.closed = true
    this.emitter = null
  }

  public on(
    name: Event.Connect,
    handler: ConnectEventHandler,
    context?: any
  ): void
  public on(
    name: Event.Message,
    handler: MessageEventHandler,
    context?: any
  ): void
  public on(
    name: Event.Disconnect,
    handler: DisconnectEventHandler,
    context?: any
  ): void
  public on(name: Event.Error, handler: ErrorEventHandler, context?: any): void
  public on(
    name: string,
    handler: (...args: any) => void,
    context?: any
  ): void {
    if (this.closed) {
      throw new Error("client already closed")
    }

    this.emitter.on(name, handler, context)
  }

  public once(
    name: Event.Connect,
    handler: ConnectEventHandler,
    context?: any
  ): void
  public once(
    name: Event.Message,
    handler: MessageEventHandler,
    context?: any
  ): void
  public once(
    name: Event.Disconnect,
    handler: DisconnectEventHandler,
    context?: any
  ): void
  public once(
    name: Event.Error,
    handler: ErrorEventHandler,
    context?: any
  ): void
  public once(
    name: string,
    handler: (...args: any) => void,
    context?: any
  ): void {
    if (this.closed) {
      throw new Error("client already closed")
    }

    this.emitter.once(name, handler, context)
  }

  public off(name: Event.Connect, handler?: ConnectEventHandler): void
  public off(name: Event.Message, handler?: MessageEventHandler): void
  public off(name: Event.Disconnect, handler?: DisconnectEventHandler): void
  public off(name: Event.Error, handler?: ErrorEventHandler): void
  public off(name: string, handler?: (...args: any) => void): void {
    if (this.closed) {
      throw new Error("client already closed")
    }

    this.emitter.off(name, handler)
  }
}
