"use strict"

function createUpdateExpression(update) {
  const expr = { str: 'set', attr: { names: {}, values: {} } }
  const __recursive = (update, parent) => {
    Object.keys(update).forEach( (prop, index) => {
      /*
        create p which is the lhs in the expression equation
        if parent exist, attribute name will be the child
      */
      const p = parent? parent.replace(/(#|\.)/g,'') : ''
      const child = `#${p}p${index}`
      expr.attr.names[child] = prop
      const path = `${parent? parent+'.' : ''}${child}`
      const val = update[prop]

      if ({}.toString.call(val)  === '[object Object]') {
        __recursive(val, path)
      } else {
        /* create the value which is the rhs in the expression equation */
        const v = `:${p}v${index}`
        expr.str += ` ${path} = ${v},`
        expr.attr.values[v] = val
      }
    })
  }
  __recursive(update, null)
  expr.str = expr.str.replace(/,$/,'')
  return expr
}

function createKeyConditionExpression(keys) {
  const expr = { str: '', attr: { names: {}, values: {} } }
  const _keys = ['#hkey', '#rkey']
  const _vals = [':hval', ':rval']
  Object.keys(keys).forEach( (key, index) => {
    const value = keys[key].replace(/\s.*$/,` ${_vals[index]}`)
    expr.attr.names[_keys[index]] = key
    expr.attr.values[_vals[index]] = keys[key].match(/\s.*$/)[0].trim()
    if (index > 0) { expr.str += ' and ' }
    expr.str += `${_keys[index]} ${value}`
  })
  return expr
}

/** Class representing a database driver */
class DatabaseDriver {
  constructor(docClient, table, options) {
    this.table = table
    this.index = (options && options.index) || null
    this.docClient = docClient
  }

  /**
   * Find one or more documents using keys
   * @param {Object} keys
   * @return Promise
   * ex. find({ uid: '= cafeguy', publish: '> 1570198352200' })
   *          .then( docs => console.log(docs) )
   *          .catch( err => console.log(err) )
   */
  find(keys) {
    return new Promise( (resolve, reject) => {
      const expr = createKeyConditionExpression(keys)
      const params = {
        TableName: this.table,
        KeyConditionExpression: expr.str,
        ExpressionAttributeNames: expr.attr.names,
        ExpressionAttributeValues: expr.attr.values
      }
      if (this.index) { params.IndexName = this.index }
      this.docClient.query( params, (err, data) => {
        if (err) {
          reject(err)
        } else {
          resolve(data.Items)
        }
      })
    })
  }

  /**
   * Insert one document to the Collection/Table
   * @param {Object} doc
   * @return Promise
   */
  insert(doc) {
    return new Promise( (resolve, reject) => {
      this.docClient.put({ TableName: this.table, Item: doc }, (err) => {
        if (err) {
          reject(err)
        } else {
          resolve(doc)
        }
      })
    })
  }

  /**
   * Update one or more documents by keys
   * @param {Object} keys
   * @param {Object} update
   * @return Promise
   */
  update(keys, update) {
    return new Promise( (resolve, reject) => {
      const expr = createUpdateExpression(update)
      // resolve(expr); return
      this.docClient.update({
        TableName: this.table,
        Key: keys,
        UpdateExpression: expr.str,
        ExpressionAttributeNames: expr.attr.names,
        ExpressionAttributeValues: expr.attr.values,
        ReturnValues:"UPDATED_NEW"
      }, (err) => {
        if (err) {
          reject(err)
        } else {
          resolve(update)
        }
      })
    })
  }
}

class DatabseHelper {
  constructor(config) {
    const AWS = require('aws-sdk')
    if (config && config.aws && config.aws.region && config.aws.endpoint) {
      AWS.config.update({ region: config.aws.region, endpoint: config.aws.endpoint })
    }
    const apiVersion = (config && config.apiVersion) || '2012-08-10'
    this.docClient = new AWS.DynamoDB.DocumentClient({ apiVersion })
    this.drivers = {
      batchGet: this.batchGet.bind(this),
      batchWrite: this.batchWrite.bind(this)
    }
  }
  /*
    table can be a String or an Array of String or Object
    ex. addTable('USERS', { indexes: ['LOGIN'] })
        addTable(['REALMS', 'APPS'])
        addTable([{ table: 'USERS', indexes: ['LOGIN'] }, 'REALMS', 'APPS'])
  */
  addTable(table, options) {
    if ({}.toString.call(table)  === '[object Array]') {
      table.forEach( t => {
        if ({}.toString.call(table)  === '[object Object]') {
          this.drivers[t.table] = new DatabaseDriver(this.docClient, t.table)
          if (t.indexes) {
            t.indexes.forEach( index => {
              this.drivers[index] = new DatabaseDriver(this.docClient, t.table, { index: t.index })
            })
          }
        } else {
          this.drivers[t] = new DatabaseDriver(this.docClient, t)
        }
      })
    } else {
      this.drivers[table] = new DatabaseDriver(this.docClient, table)
      if (options && options.indexes) {
        options.indexes.forEach( index => this.drivers[index] = new DatabaseDriver(this.docClient, table, { index }))
      }
    }
    return this
  }
  batchGet() {}
  batchWrite() {}
}

module.exports = DatabseHelper
