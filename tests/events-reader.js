const test = require('tape')
const EventsReader = require('../lib/EventsReader.js')
const ReactiveDao = require('@live-change/dao')

const dbName = 'event-reader.test'

test("EventsReader", t => {
  t.plan(6)

  let server, client, eventsReader, connection, eventCallback, savedPosition

  t.test('connect to server and create database', async t => {
    t.plan(1)
    server = await require('@live-change/db-server/tests/getServer.js')
    client = await server.connect(1, 5)
    await client.request(['database', 'createDatabase'], dbName)
    await client.request(['database', 'createLog'], dbName, 'events')
    t.pass('opened')
  })

  t.test('create and start event reader', async t => {
    t.plan(1)
    eventsReader = new EventsReader('',
        (position, limit) =>
            client.observable(['database', 'logRange', dbName, 'events', { gt: position, limit }],
                ReactiveDao.ObservableList),
        async event => eventCallback ? eventCallback(event) : 'ok',
        (position) => { savedPosition = position; console.log("SAVE POS", position) },
        2
    )
    await eventsReader.start()
    t.pass('events reader started')
  })

  t.test("test 10 events", async t => {
    t.plan(1)
    let counter = 0
    eventCallback = async (event) => {
      if(event.type == 'inc') counter ++
      console.log("COUNTER", counter)
      if(counter == 10) t.pass('10 events processed')
    }
    for(let i = 0; i < 10; i++) {
      await client.request(['database', 'putLog'], dbName, 'events', { type: 'inc' })
    }
  })

  t.test("stop reader", async t => {
    t.plan(1)
    eventsReader.dispose()
    if(savedPosition) {
      t.pass('reader stopped and position saved')
    } else {
      t.fail('position not saved')
    }
  })

  t.test("push events while reader is stopped", async t => {
    t.plan(1)
    eventCallback = async (event) => {
      t.fail("reaction for event when reader is stopped")
    }
    for(let i = 0; i < 10; i++) {
      await client.request(['database', 'putLog'], dbName, 'events', { type: 'inc' })
    }
    t.pass("no reaction for events while it's stopped")
  })

  t.test('restart event reader and read events', async t => {
    t.plan(2)
    eventsReader = new EventsReader('',
        (position, limit) =>
            client.observable(['database', 'logRange', dbName, 'events', { gt: position, limit }],
                ReactiveDao.ObservableList),
        async event => eventCallback ? eventCallback(event) : 'ok',
        (position) => { savedPosition = position; console.log("SAVE POS", position) },
        2
    )
    let counter = 0
    eventCallback = async (event) => {
      if(event.type == 'inc') counter ++
      console.log("COUNTER", counter)
      if(counter == 10) t.pass('10 events processed')
    }
    await eventsReader.start()
    t.pass('events reader started')
  })
})
