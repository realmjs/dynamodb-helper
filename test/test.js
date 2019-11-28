"use strict"

require('dotenv').config()

const DatabaseHelper = require('../src/main')

const dbh = new DatabaseHelper({
  aws: { region: process.env.AWS_REGION, endpoint: process.env.AWS_ENDPOINT },
})

dbh.addTable('USERS', {indexes: ['LOGIN']})
dbh.addTable(['ENROLL'])

/* Start execution time measurement */
const hrstart = process.hrtime()

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

dbh.drivers.LOGIN.find({ username: '= tester@team.com' })
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
  const hrend = process.hrtime(hrstart)
  console.log('\n-----------------------------------------------------------')
  console.log('-  Execution time (hr): %ds %dms', hrend[0], hrend[1] / 1000000)
  console.log('-----------------------------------------------------------')
  console.log('Output results:')
  console.log(data)
}
