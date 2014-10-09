var tape = require('tape')
var memdown = require('memdown')
var levelup = require('levelup')
var concat = require('concat-stream')
var scuttleup = require('./')

var init = function() {
  return scuttleup(levelup('test', {db:memdown}), {valueEncoding:'utf-8'})
}

var toEntry = function(data) {
  return data.entry
}

tape('createReadStream', function(t) {
  var log = init()
  var expects = ['test1', 'test2']

  log.append('test1', function() {
    log.append('test2', function() {
      log.createReadStream().pipe(concat(function(list) {
        t.same(list.map(toEntry), expects)
        t.end()
      }))
    })
  })
})

tape('createReadStream live', function(t) {
  var log = init()
  var expects = ['test1', 'test2']

  log.createReadStream({live:true})
    .on('data', function(data) {
      t.same(data.entry, expects.shift())
      if (!expects.length) t.end()
    })

  log.append('test1', function() {
    log.append('test2')
  })
})

tape('createWriteStream', function(t) {
  var log = init()
  var log2 = init()
  var expects = ['test1', 'test2']

  log.append('test1', function() {
    log.append('test2', function() {
      log.createReadStream().pipe(log2.createWriteStream()).on('finish', function() {
        log2.createReadStream().pipe(concat(function(list) {
          t.same(list.map(toEntry), expects)
          t.end()
        }))
      })
    })
  })
})

tape('basic replication', function(t) {
  var a = init()
  var b = init()

  var as = a.createReplicationStream()
  var bs = b.createReplicationStream()

  as.pipe(bs).pipe(as)

  a.append(new Buffer('i am a'))
  b.append(new Buffer('i am b'))

  var sort = function(a, b) {
    return a.entry.toString().localeCompare(b.entry.toString())
  }

  setTimeout(function() {
    a.createReadStream().pipe(concat(function(alist) {
      b.createReadStream().pipe(concat(function(blist) {
        t.same(alist.sort(sort), blist.sort(sort), 'a and b replicates')
        t.end()
      }))
    }))
  }, 100)
})

tape('pull replication', function(t) {
  var a = init()
  var b = init()

  var as = a.createReplicationStream({mode:'pull'})
  var bs = b.createReplicationStream()

  as.pipe(bs).pipe(as)

  a.append(new Buffer('i am a'))
  b.append(new Buffer('i am b'))

  var sort = function(a, b) {
    return a.entry.toString().localeCompare(b.entry.toString())
  }

  setTimeout(function() {
    a.createReadStream().pipe(concat(function(alist) {
      b.createReadStream().pipe(concat(function(blist) {
        t.same(alist.length, 2)
        t.same(blist.length, 1)
        t.same(blist[0].entry, new Buffer('i am b'))
        t.end()
      }))
    }))
  }, 100)
})

tape('push replication', function(t) {
  var a = init()
  var b = init()

  var as = a.createReplicationStream()
  var bs = b.createReplicationStream({mode:'push'})

  as.pipe(bs).pipe(as)

  a.append(new Buffer('i am a'))
  b.append(new Buffer('i am b'))

  var sort = function(a, b) {
    return a.entry.toString().localeCompare(b.entry.toString())
  }

  setTimeout(function() {
    a.createReadStream().pipe(concat(function(alist) {
      b.createReadStream().pipe(concat(function(blist) {
        t.same(alist.length, 2)
        t.same(blist.length, 1)
        t.same(blist[0].entry, new Buffer('i am b'))
        t.end()
      }))
    }))
  }, 100)
})

tape('get entry', function(t) {
  var a = init()

  a.on('append', function(data) {
    a.entry(data.peer, data.seq, function(err, entry) {
      t.error(err, 'no err')
      t.same(entry, new Buffer('hello world'))
      t.end()
    })
  })

  a.append(new Buffer('hello world'))
})