const test = require('tape')
const EventSourcing = require('../lib/EventSourcing.js')

const dbName = 'event-sourcing.test'

test("event sourcing", t => {
  t.plan(6)

  let server, client, eventSourcing, connection, eventCallback

  t.test('connect to server and create database', async t => {
    t.plan(1)
    server = await require('@live-change/db-server/tests/getServer.js')
    client = await server.connect(1, 5)
    await client.request(['database', 'createDatabase'], dbName)
    t.pass('opened')
  })

  t.test('create and start event sourcing', async t => {
    t.plan(1)
    eventSourcing = new EventSourcing(client, dbName, 'events', 'testConsumer')
    eventSourcing.addEventHandler('inc', (ev) => eventCallback(ev) )
    await eventSourcing.start()
    t.pass('events reader started')
  })

  t.test("test 20 events in 10 buckets", async t => {
    t.plan(1)
    let counter = 0
    eventCallback = async (event) => {
      if(event.type == 'inc') counter ++
      console.log("COUNTER", counter)
      if(counter == 20) t.pass('20 events in 10 buckets processed')
    }
    for(let i = 0; i < 10; i++) {
      await client.request(['database', 'putLog'], dbName, 'events', { type: 'bucket', events: [
          { type: 'inc' }, { type: 'inc' }
      ]})
      console.log("BUCKET PUSHED")
    }
  })

  t.test("stop event sourcing", async t => {
    t.plan(1)
    await eventSourcing.dispose()
    t.pass("event sourcing disposed")
  })

  t.test("push events while reader is stopped", async t => {
    t.plan(1)
    eventCallback = async (event) => {
      t.fail("reaction for event when reader is stopped")
    }
    for(let i = 0; i < 10; i++) {
      await client.request(['database', 'putLog'], dbName, 'events', { type: 'bucket', events: [
          { type: 'inc' }, { type: 'inc' }
      ]})
    }
    t.pass("no reaction for events while it's stopped")
  })

  t.test('restart event reader and read events', async t => {
    t.plan(2)
    eventSourcing = new EventSourcing(client, dbName, 'events', 'testConsumer')
    eventSourcing.addEventHandler('inc', (ev) => eventCallback(ev) )
    let counter = 0
    eventCallback = async (event) => {
      if(event.type == 'inc') counter ++
      console.log("COUNTER", counter)
      if(counter == 20) t.pass('20 events in 10 buckets processed')
    }
    await eventSourcing.start()
    t.pass('events reader started')
  })
})
