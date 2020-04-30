"use strict"

const time = require('./time-measure')

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
        if (val === true || val === false) {
          expr.attr.values[v] = val
        } else {
          expr.attr.values[v] = isNaN(val) ? val : parseFloat(val)
        }
      }
    })
  }
  __recursive(update, null)
  expr.str = expr.str.replace(/,$/,'')
  return expr
}

/*
  ex:
  scheme = {
    'content': {
      'questions': {
        '$_value'
      }
    }
  }
  update = {
    'content': {
      'questions': {
        '0' : {
          'userAnswer': { '$1': 'Dragonborn' }
        }
      }
    }
  }
  expr = {
    str: set #content.#questions =
    attr: {
      name: {
        '#content': 'content',
        '#questions': 'question',
      }
      value: {
        ':value': {
          '0' : {
            'userAnswer': { '$1': 'Dragonborn' }
          }
        }
      }
    }
  }
*/
function createSchemeUpdateExpression(update, schema) {
  if (schema) {
    const expr = { str: 'set ', attr: { names: {}, values: {} } };
    const __recursive = (update, schema) => {
      for (let prop in schema) {
        const __schema = schema[prop];

        if (/^\$_/.test(prop)) {
          const _str = expr.str.replace(/^set/,'');
          Object.keys(update).forEach ( (p, index) => {
            expr.attr.names[`#p${p}`] = p;
            expr.attr.values[`:v${p}`] = update[p];
            if (index === 0) {
              expr.str += `#p${p} = :v${p}, `;
            } else {
              expr.str += `${_str}#p${p} = :v${p}, `;
            }
          })
        } else {
          expr.attr.names[`#${prop}`] = prop;
          expr.str += `#${prop}.`;
        }

        if ({}.toString.call(__schema)  === '[object Object]') {
          __recursive(update[prop], __schema);
        }
      }
    }
    __recursive(update, schema);
    expr.str = expr.str.trim().replace(/,$/,'');
    return expr;
  } else {
    const expr = { str: 'set', attr: { names: {}, values: {} } }
    // set
    for (let key in update) {
      const p = `#${key}`;
      const v = `:${key}`;
      expr.attr.names[p] = key;
      expr.attr.values[v] = update[key];
      expr.str += ` ${p} = ${v},`;
    }
    expr.str = expr.str.replace(/,$/,'');
    return expr;
  }
}

function createRemoveExpression(attributes) {
  const expr = { str: 'remove', attr: {} }
  attributes.forEach(key => {
    const attrs = key.split('.')
    attrs.forEach(a => { expr.attr[`#${a}`] = a })
    expr.str += ' ' + attrs.map(a => `#${a}`).join('.') + ','
  })
  expr.str = expr.str.replace(/,$/,'')
  return expr
}

function createKeyConditionExpression(keys) {
  const expr = { str: '', attr: { names: {}, values: {} } }
  const _keys = ['#hkey', '#rkey']
  const _vals = [':hval', ':rval']
  Object.keys(keys).forEach( (key, index) => {
    const _v = keys[key].trim()
    // special process for BETWEEN keywork in range key
    if (/^between/i.test(_v)) {
      const s = _v.split(" ").filter(a => a.length > 0)
      expr.attr.names[_keys[index]] = key
      expr.attr.values[':rval1'] = isNaN(s[1])? s[1] : parseFloat(s[1])
      expr.attr.values[':rval2'] = isNaN(s[3])? s[3] : parseFloat(s[3])
      const value = `BETWEEN :rval1 AND :rval2`
      if (index > 0) { expr.str += ' and ' }
      expr.str += `${_keys[index]} ${value}`
      return
    }
    const _v_op = _v.match(/^\W+/)[0].trim()
    const _v_val = _v.match(/^\W+(.*)/)[1].trim()
    const value = `${_v_op} ${_vals[index]}`
    expr.attr.names[_keys[index]] = key
    expr.attr.values[_vals[index]] = isNaN(_v_val) ? _v_val : parseFloat(_v_val)
    if (index > 0) { expr.str += ' and ' }
    expr.str += `${_keys[index]} ${value}`
  })
  return expr
}

function createProjectionExpression(projection) {
  if (!projection) {
    return { str: null, attr: {} }
  }
  const expr = { str: '', attr: {} }
  projection.forEach( p => {
    expr.attr[`#${p}`] = p
  })
  expr.str = projection.map( p => `#${p}`).join(',')
  return expr
}

function createFilterExpression(filter) {
  const expr = { str: '', attr: { names:{}, values: {} } }
  filter && Object.keys(filter).forEach( (key, index) => {
    const _cond = filter[key].trim()
    const _cond_op = _cond.match(/^\W+/)[0].trim()
    const _cond_val = _cond.match(/^\W+(.*)/)[1].trim()
    expr.attr.names[`#k_${key}_f`] = key
    expr.attr.values[`:v_${key}_f`] = isNaN(_cond_val) ? _cond_val : parseFloat(_cond_val)
    if (index > 0) { expr.str += ' and ' }
    expr.str += `#k_${key}_f ${_cond_op} :v_${key}_f`
  })
  return expr
}

/** Class representing a database driver */
class DatabaseDriver {
  constructor(docClient, table, options) {
    this.table = table
    this.index = (options && options.index) || null
    this.docClient = docClient
    for (let prop in options) {
      this[prop] = options[prop]
    }
  }

  /**
   * Find one or more documents using keys
   * @param {Object} keys
   * * @param {Object} projection
   * @return Promise
   * ex. find({ uid: '= cafeguy', publish: '> 1570198352200' })
   *          .then( docs => console.log(docs) )
   *          .catch( err => console.log(err) )
   */
  find(keys, projection, filter, options) {
    return new Promise( (resolve, reject) => {
      const expr = createKeyConditionExpression(keys)
      const prj = createProjectionExpression(projection)
      const fil = createFilterExpression(filter)
      const params = {
        TableName: this.table,
        KeyConditionExpression: expr.str,
        ExpressionAttributeNames: {...expr.attr.names, ...prj.attr, ...fil.attr.names},
        ExpressionAttributeValues: {...expr.attr.values, ...fil.attr.values},
        ...options
      }
      if (this.index) { params.IndexName = this.index }
      if (projection) { params.ProjectionExpression = prj.str }
      if (filter) { params.FilterExpression = fil.str }
      const t = time.measure.start()
      this.docClient.query( params, (err, data) => {
        time.measure.end(t, `Find item in table ${this.table}`)
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
      const t = time.measure.start()
      this.docClient.put({ TableName: this.table, Item: doc }, (err) => {
        time.measure.end(t, `Insert item into table ${this.table}`)
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
      const t = time.measure.start()
      this.docClient.update({
        TableName: this.table,
        Key: keys,
        UpdateExpression: expr.str,
        ExpressionAttributeNames: expr.attr.names,
        ExpressionAttributeValues: expr.attr.values,
        ReturnValues:"UPDATED_NEW"
      }, (err) => {
        time.measure.end(t, `Update item in table ${this.table}`)
        if (err) {
          reject(err)
        } else {
          resolve(update)
        }
      })
    })
  }

  /**
   * Similar to update, but do not perform update in each child
   * @param {Object} keys
   * @param {Object} prop
   * @return Promise
   */
  set(keys, update, schema) {
    return new Promise( (resolve, reject) => {
      const expr = createSchemeUpdateExpression(update, schema);
      const t = time.measure.start()
      this.docClient.update({
        TableName: this.table,
        Key: keys,
        UpdateExpression: expr.str,
        ExpressionAttributeNames: expr.attr.names,
        ExpressionAttributeValues: expr.attr.values,
        ReturnValues:"UPDATED_NEW"
      }, (err) => {
        time.measure.end(t, `Set item in table ${this.table}`)
        if (err) {
          reject(err)
        } else {
          resolve(update)
        }
      })
    })
  }

  /**
   * Remove one item or attributes of item from a table
   * To remove more than one item, use batchWrite instead
   * If attrs is not provided, remove item
   * Currently, it does not support remove from indexed
   * @param {Object} keys
   * @param {Array} attrs
   * @return Promise
   */
  remove(keys, attrs) {
    return new Promise((resolve, reject) => {
      const t = time.measure.start()
      if (attrs && attrs.length > 0) {
        const expr = createRemoveExpression(attrs)
        this.docClient.update({
          TableName: this.table,
          Key: keys,
          UpdateExpression: expr.str,
          ExpressionAttributeNames: expr.attr,
          ReturnValues:"UPDATED_NEW"
        }, (err) => {
          time.measure.end(t, `Remove attribute of item from table ${this.table}`)
          if (err) {
            reject(err)
          } else {
            resolve(attrs)
          }
        })
      } else {
        this.docClient.delete({ TableName: this.table, Key: keys }, (err, data) => {
          time.measure.end(t, `Remove item from table ${this.table}`)
          if (err) {
            reject(err)
          } else {
            resolve()
          }
        })
      }
    })
  }

  /**
   * Fetch all items from a table
   * @return Promise
   */
  fetch() {
    return new Promise((resolve, reject) => {
      const t = time.measure.start()
      this.docClient.scan({ TableName: this.table }, (err, data) => {
        time.measure.end(t, `Fetch (scan) from table ${this.table}`)
        if (err) {
          reject(err)
        } else {
          resolve(data.Items)
        }
      })
    })
  }
}

class DatabseHelper {
  constructor(config) {
    const AWS = require('aws-sdk')
    if (config && config.aws && config.aws.region && config.aws.endpoint) {
      AWS.config.update(config.aws)
    }
    const apiVersion = (config && config.apiVersion) || '2012-08-10'
    this.docClient = new AWS.DynamoDB.DocumentClient({ apiVersion })
    this.drivers = {
      batchGet: this.batchGet.bind(this),
      batchWrite: this.batchWrite.bind(this)
    }
    this.config = config
    time.measure.config({ measureExecutionTime: config.measureExecutionTime })
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
        if ({}.toString.call(t)  === '[object Object]') {
          this.drivers[t.table] = new DatabaseDriver(this.docClient, t.table, { ...this.config })
          if (t.indexes) {
            t.indexes.forEach( index => {
              this.drivers[index] = new DatabaseDriver(this.docClient, t.table, { index: index, ...this.config })
            })
          }
        } else {
          this.drivers[t] = new DatabaseDriver(this.docClient, t, { ...this.config })
        }
      })
    } else {
      this.drivers[table] = new DatabaseDriver(this.docClient, table, { ...this.config })
      if (options && options.indexes) {
        options.indexes.forEach( index => this.drivers[index] = new DatabaseDriver(this.docClient, table, { index, ...this.config }))
      }
    }
    return this
  }
  /*
    get item from multiple tables in batch mode, using key
    Notes: batchGet does not support INDEX table
           batchGet curently does not support expression in projection, therefore avoid to have projection contain dynamodb keywords such as roles...
           batchGet required both hash and range keys for composite primary key
    ex. dbh.drivers.batchGet({
      USERS: { keys: {uid: 'cafe-guy'}, projection: ['rewards'] },
      ORDERS: { keys: {uid: 'cafe-guy', orderId: '123456'} }
    })
  */
  batchGet(params) {
    const RequestItems = {}
    for (let table in params) {
      if (params[table].keys && params[table].keys.length > 0) {
        RequestItems[table] = {
          Keys: params[table].keys
        }
      }
      if (params[table].projection) {
        const prj = createProjectionExpression(params[table].projection)
        RequestItems[table].ProjectionExpression = prj.str
        RequestItems[table].ExpressionAttributeNames = prj.attr
      }
    }
    return new Promise( (resolve, reject) => {
      const t = time.measure.start()
      this.docClient.batchGet({ RequestItems }, (err, data) => {
        time.measure.end(t, `BatchGet item from tables: ${Object.keys(params)}`)
        if (err) {
          reject(err)
        } else {
          resolve(data.Responses)
        }
      })
    })
  }
  /*
    write/remove items to multiple tables in batch mode,
    ex. dbh.drivers.batchWrite({
        ENROLL: {
          insert: [{ uid: 'usr1', courseId: 'e1'},{ uid: 'usr1', courseId: 'e2'}],
          remove: [{ uid: 'usr2', courseId: 'e1'},{ uid: 'usr2', courseId: 'e2'}]
        }
      })
  */
  batchWrite(params) {
    const RequestItems = {}
    for (let table in params) {
      RequestItems[table] = []
      if (params[table].insert) {
        const items = params[table].insert
        RequestItems[table].push(...items.map(item => {
          return {
            PutRequest: { Item: item }
          }
        }))
      }
      if (params[table].remove) {
        const keys = params[table].remove
        RequestItems[table].push(...keys.map(key => {
          return {
            DeleteRequest: { Key: key }
          }
        }))
      }
    }
    return new Promise( (resolve, reject) => {
      const t = time.measure.start()
      this.docClient.batchWrite({ RequestItems }, (err, data) => {
        time.measure.end(t, `BatchWrite to tables: ${Object.keys(params)}`)
        if (err) {
          reject(err)
        } else {
          resolve(data.UnprocessedItems)
        }
      })
    })
  }
}

module.exports = DatabseHelper
