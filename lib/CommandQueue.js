const ReactiveDao = require('@live-change/dao')

class CommandQueue {
  constructor(connection, database, tableName, config = {}) {
    this.connection = connection
    this.database = database
    this.tableName = tableName
    this.indexName = tableName + '_new'
    this.config = config
    this.observable = null
    this.disposed = false
    this.resolveStart = null
    this.commandsStarted = new Map()
    this.allCommandHandlers = []
    this.commandTypeHandlers = new Map()
  }
  async dispose() {
    this.disposed = true
    if(this.observable) {
      this.observable.unobserve(this)
      this.observable = null
    }
  }
  async start() {
    await this.connection.request(['database', 'createTable'], this.database, this.tableName).catch(e => 'ok')
    await this.connection.request(['database', 'createIndex'], this.database, this.indexName, `(${
        async function(input, output, { tableName }) {
          await input.table(tableName).onChange((obj, oldObj) =>
              output.change(
                  obj && obj.state == 'new' ? obj : null,
                  oldObj && oldObj.state == 'new' ? oldObj : null
              )
          )
        }
    })`, { tableName: this.tableName }).catch(e => 'ok')
    this.observable = this.connection.observable(
        ['database', 'indexRange', this.database, this.indexName, { limit: this.config.limit || 100 }],
        ReactiveDao.ObservableList)
    this.observable.observe(this)
    return new Promise((resolve, reject) => {
      this.resolveStart = { resolve, reject }
    })
  }
  async handleCommand(command) {
    if(this.config.filter && !this.config.filter(command)) return 'filtered'
    if(command.state != 'new') return
    if(this.commandsStarted.has(command.id)) return
    this.commandsStarted.set(command.id, command)
    try {
      let handled = false
      let result = null
      let commandHandlers = this.commandTypeHandlers.get(command.type) || []
      for(let handler of commandHandlers) {
        result = handler(command)
        if(result != 'ignored') {
          handled = true
          break
        }
      }
      if(!handled) {
        for(let handler of this.allCommandHandlers) {
          const result = handler(command)
          if(result != 'ignored') {
            handled = true
            break
          }
        }
      }
      if(!handled) {
        console.error(`Command handler for type ${command.type} not found`)
        throw new Error("notHandled")
      }
      return Promise.resolve(result).then(async result => {
        await this.connection.request(['database', 'update'], this.database, this.tableName, command.id, [
          { op: 'merge', property: null, value: { state: 'done', result } }
        ])
        // hold it for one second in case of delayed event:
        setTimeout(() => this.commandsStarted.delete(command.id), 1000)
      }).catch(async error => {
        return this.connection.request(['database', 'update'], this.database, this.tableName, command.id, [
          { op: 'merge', property: null, value: { state: 'failed', error } }
        ])
        // hold it for one second in case of delayed event:
        setTimeout(() => this.commandsStarted.delete(command.id), 1000)
      })
    } catch(e) {
      console.error(`COMMAND ${command} HANDLING ERROR`, e, ' => STOPPING!')
      this.dispose()
      throw e
    }
    return 'ok'
  }
  set(value) {
    if(this.resolveStart) {
      this.resolveStart.resolve()
      this.resolveStart = null
    }
    if(this.disposed) return
    const commands = value.slice()
    for(let command of commands) {
      this.handleCommand(command)
    }
  }
  putByField(field, id, object, oldObject) {
    if(this.disposed) return
    if(oldObject) return // Object changes ignored
    this.handleCommand(object)
  }
  error(error) {
    if(this.resolveStart) {
      this.resolveStart.reject(error)
      this.resolveStart = null
    }
    console.error("COMMAND QUEUE READ ERROR", error)
    this.dispose()
  }
  addAllCommandsHandler(handler) {
    this.allCommandHandlers.push(handler)
  }
  addCommandHandler(commandType, handler) {
    let handlers = this.commandTypeHandlers.get(commandType)
    if(!handlers) {
      handlers = []
      this.commandTypeHandlers.set(commandType, handlers)
    }
    handlers.push(handler)
  }
}

module.exports = CommandQueue
