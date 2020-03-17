const ReactiveDao = require('@live-change/dao')

const test = require('tape')
const CommandQueue = require('../lib/CommandQueue.js')

const dbName = 'command-queue.test'
const tableName = 'commands'

test("command queue", t => {
  t.plan(6)

  let server, client, commandQueue, connection, commandCallback

  t.test('connect to server and create database', async t => {
    t.plan(1)
    server = await require('@live-change/db-server/tests/getServer.js')
    client = await server.connect(1, 5)
    await client.request(['database', 'createDatabase'], dbName)
    t.pass('opened')
  })

  t.test('create and start command queue', async t => {
    t.plan(1)
    commandQueue = new CommandQueue(client, dbName, tableName)
    commandQueue.addCommandHandler('inc', (command) => {
      if(commandCallback) return commandCallback(command)
      return 'ok'
    })
    await commandQueue.start()
    t.pass('commands reader started')
  })

  t.test("test 20 commands in 10 buckets", async t => {
    t.plan(20)
    let counter = 0
    commandCallback = async (command) => {
      if(command.type == 'inc') counter ++
      t.pass(`counter incrased to ${counter} with command ${command.id}`)
    }
    for(let i = 0; i < 10; i++) {
      const id = `${(Math.random()*1000000) | 0}`
      const objectObservable = client.observable(['database', 'tableObject', dbName, tableName, id],
          ReactiveDao.ObservableValue)
      await client.request(['database', 'put'], dbName, tableName, { id, type: 'inc', state: 'new' })
      console.log("COMMAND PUSHED")
      objectObservable.observe((signal, value) => {
        console.log(`COMMAND ${id} OBJECT SIGNAL`, signal, value)
        if(signal == 'set' && value && value.state == 'done') t.pass(`command ${value.id} finished`)
      })
    }
  })

  t.test("stop command queue", async t => {
    t.plan(1)
    await commandQueue.dispose()
    t.pass("command queue disposed")
  })

  t.test("push commands while reader is stopped", async t => {
    t.plan(1)
    commandCallback = async (command) => {
      t.fail("reaction for command when reader is stopped")
    }
    for(let i = 0; i < 10; i++) {
      const id = `${(Math.random()*1000000) | 0}`
      await client.request(['database', 'put'], dbName, tableName,  { id, type: 'inc', state: 'new' })
    }
    t.pass("no reaction for commands while it's stopped")
  })

  t.test('restart command reader and read commands', async t => {
    t.plan(2)
    commandQueue = new CommandQueue(client, dbName, tableName, 'testConsumer')
    commandQueue.addCommandHandler('inc', (command) => {
      if(commandCallback) return commandCallback(command)
      return 'ok'
    })
    let counter = 0
    commandCallback = async (command) => {
      if(command.type == 'inc') counter ++
      console.log("COUNTER", counter)
      if(counter == 10) t.pass('10 commands processed')
    }
    await commandQueue.start()
    t.pass('commands reader started')
  })
})
