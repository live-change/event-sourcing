const EventsReader = require('./EventsReader.js')
const ReactiveDao = require('@live-change/dao')

class EventSourcing {
  constructor(connection, database, logName, consumerName, config = {}) {
    this.connection = connection
    this.database = database
    this.logName = logName
    this.consumerName = consumerName
    this.consumerId = this.logName + '.' + this.consumerName
    this.config = config
    this.reader = null
    this.lastPositionSave = Date.now()
    this.lastSavedPosition = 0
    this.allEventHandlers = []
    this.eventTypeHandlers = new Map()
  }
  async dispose() {
    if(this.reader) this.reader.dispose()
    await this.saveState()
  }
  async start() {
    await this.connection.request(['database', 'createTable'], this.database, 'eventConsumers').catch(e => 'ok')
    await this.connection.request(['database', 'createLog'], this.database, this.logName).catch(e => 'ok')
    this.state = await this.connection.get(
        ['database', 'tableObject', this.database, 'eventConsumers', this.consumerId])
    if(!this.state) {
      this.state = {
        id: this.consumerId,
        position: ''
      }
      await this.saveState()
    }
    this.reader = new EventsReader(
        this.state.position,
        (position, limit) =>
            this.connection.observable(['database', 'logRange', this.database, this.logName, { gt: position, limit }],
                ReactiveDao.ObservableList),
        (event) => this.handleEvent(event, event),
        (position) => {
          this.state.position = position
          this.savePosition(position)
        },
        this.config.fetchSize || 100
    )
    await this.reader.start()
  }
  async saveState() {
    await this.connection.request(
        ['database', 'put'], this.database, 'eventConsumers', this.state)
  }
  async handleEvent(event, mainEvent) {
    if(this.config.filter && !this.config.filter(event)) return 'filtered'
    if(event.type == 'bucket') { // Handle buckets
      for(const subEvent of event.events) {
        await this.handleEvent(subEvent, mainEvent)
      }
      return
    }
    try {
      let handled = false
      let eventHandlers = this.eventTypeHandlers.get(event.type) || []
      for(let handler of eventHandlers) {
        const result = await handler(event, mainEvent)
        if(result != 'ignored') handled = true
      }
      for(let handler of this.allEventHandlers) {
        const result = await handler(event, mainEvent)
        if(result != 'ignored') handled = true
      }
      if(!handled) {
        throw new Error("notHandled")
      }
    } catch(e) {
      console.error(`EVENT ${event} HANDLING ERROR`, e, ' => STOPPING!')
      this.dispose()
      throw e
    }
    return 'ok'
  }
  async savePosition() {
    if(this.lastSavedPosition == this.state.position) return
    if(this.lastPositionSave - Date.now() <= (this.config.saveThrottle || 1000)) {
      setTimeout(() => this.savePosition, this.config.saveThrottle)
      return
    }
    this.saveState()
  }
  addAllEventsHandler(handler) {
    this.allEventHandlers.push(handler)
  }
  addEventHandler(eventType, handler) {
    let handlers = this.eventTypeHandlers.get(eventType)
    if(!handlers) {
      handlers = []
      this.eventTypeHandlers.set(eventType, handlers)
    }
    handlers.push(handler)
  }
}

module.exports = EventSourcing