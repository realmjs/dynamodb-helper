"use strict"

require('dotenv').config()

const DatabaseHelper = require('../src/main')

const aws = { region: process.env.REGION, endpoint: process.env.ENDPOINT }
if (process.env.PROXY) {
  console.log('# User proxy-agent')
  const proxy = require('proxy-agent')
  aws.httpOptions = { agent: proxy(process.env.PROXY) }
}

const dbh = new DatabaseHelper({
  aws,
  measureExecutionTime: true
})

dbh.addTable('USERS', {indexes: ['LOGIN']})
dbh.addTable(['ORDER', 'MEMBER', {table: 'TEST', indexes: ['RESULT']}])

dbh.drivers.TEST.set(
  { id: 'id' },
  {
    'content': {
      'questions': {
        '0' : {
          'userAnswer': { '$1': 'Dragonborn' }
        },
        '1' : {
          'userAnswer': { '$2': 'Awesome' }
        },
      }
    }
  },
  {
    'content': {
      'questions': {
        '$_index' : '$_value'
      }
    }
  }
).then(msg => console.log(JSON.stringify(msg,null,2)))

// dbh.drivers.MEMBER.remove({ uid: '220f71d0-2800-11ea-91a8-9f528720b885' }, ['rewards.welcome', 'rewards.booster'])
// .then( expr => log(expr))
// .catch(err => console.log(err))

// dbh.drivers.ORDER.find({ uid: '= 220f71d0-2800-11ea-91a8-9f528720b885' }, ['number'], null, {Limit: 2, ScanIndexForward: false})
//          .then( expr => log(expr) )
//          .catch( err => console.log(err) )

// dbh.drivers.ENROLL.find({enrollTo: '= 220f71d0-2800-11ea-91a8-9f528720b885', courseId: '= c-01'}, ['status'])
//          .then( expr => log(expr) )
//          .catch( err => console.log(err) )

/*

dbh.drivers.ENROLL.fetch()
.then( data => log(data) )
.catch( err => console.log(err) )

dbh.drivers.batchWrite({
  ENROLL: {
    insert: [{ uid: 'usr1', courseId: 'e1'},{ uid: 'usr1', courseId: 'e2'}],
    // remove: [{ uid: 'usr1', courseId: 'e1'},{ uid: 'usr1', courseId: 'e2'}]
  }
})
.then( data => log(data) )
.catch( err => console.log(err) )

dbh.drivers.batchGet({
  CATALOG: {
    keys: [{ catalogId: 'ca-emb'}],
    projection: ['title']
  },
  COURSES: {
    keys: [{ courseId: 'emb-01' }]
  }
})
.then( data => log(data) )
.catch( err => console.log(err) )

dbh.drivers.USERS.find({ uid: '= 25d66490-f836-11e8-b04f-6b00a2182595' })
         .then( expr => log(expr) )
         .catch( err => console.log(err) )

dbh.drivers.LOGIN.find({ username: '= tester@team.com' }, ['username', 'uid', 'profile.fullName'])
         .then( expr => log(expr) )
         .catch( err => console.log(err) )

dbh.drivers.USERS.insert({
  uid: 'cafeguy',
  username: 'cafeguy@team.com',
  status: 'new',
  verified: 'false',
  profile: {
    displayName: 'Cafe',
    phone: ['123456789']
  }
})
.then( user => log(user) )
.catch( err => console.log(err) )

dbh.drivers.USERS.update({ uid: 'cafeguy'}, {
  status: 'active',
  profile: {
    displayName: 'Cafe Awesome',
    phone: ['0123456789', '9876543210'],
    email: ['cafeguy@team.com'],
    delivery: {
      address: 'Somewhere on this big city'
    }
  },
  verified: true
})
.then( data => log(data) )
.catch( err => console.log(err) )

dbh.drivers.USERS.remove({ uid: 'ebf3b030-eedd-11e9-bd64-a992427cec0f'})
.then( data => log(data) )
.catch( err => console.log(err) )

*/

function log(data) {
  console.log('Output results:')
  console.log(data)
  console.log()
}
