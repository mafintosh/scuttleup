var cuid = require('cuid')
var through = require('through2')
var duplexify = require('duplexify')
var multistream = require('multistream')
var pump = require('pump')
var peek = require('level-peek')
var lexi = require('lexicographic-integer')
var from = require('from2')
var lpstream = require('length-prefixed-stream')
var protobuf = require('protocol-buffers')
var util = require('util')
var fs = require('fs')
var events = require('events')
var path = require('path')

var messages = protobuf(fs.readFileSync(path.join(__dirname, 'schema.proto')))

var SEP = '\xff'
var CHANGE = SEP+'c'+SEP
var META = SEP+'m'+SEP

var noop = function() {}

var echo = function(val) {
  return val
}

var loadHead = function(db, cb) {
  var head = []
  var next

  var loop = function(prev) {
    peek.first(db, {start:prev, end:CHANGE+SEP}, function(err, key) {
      if (err && err.message === 'range not found' || !key) return cb(null, head)
      if (err) return cb(err)

      var peer = key.slice(CHANGE.length, key.lastIndexOf(SEP))

      peek.last(db, {end:CHANGE+peer+SEP+SEP}, function(err, key) {
        if (err) return cb(err)

        var seq = lexi.unpack(key.slice(key.indexOf(SEP, CHANGE.length)+1), 'hex')

        head.push({peer:peer, seq:seq, flushed:seq})

        loop(CHANGE+peer+SEP+SEP)
      })
    })
  }

  loop(CHANGE)
}

var load = function(id, db, cb) {
  var onid = function(id) {
    db.put(META+'id', id, {valueEncoding:'utf-8'}, function(err) {
      if (err) return cb(err)
      loadHead(db, function(err, head) {
        if (err) return cb(err)
        cb(null, id, head)
      })
    })
  }

  if (id) return onid(id)

  db.get(META+'id', {valueEncoding:'utf-8'}, function(err, id) {
    if (err && !err.notFound) return cb(err)
    onid(id || cuid())
  })
}

var encodeKey = function(peer, seq) {
  return CHANGE+peer+SEP+(seq === -1 ? SEP : lexi.pack(seq, 'hex'))
}

var decodeKey = function(key) {
  var peer = key.slice(CHANGE.length, key.indexOf(SEP, CHANGE.length))
  var seq = lexi.unpack(key.slice(key.lastIndexOf(SEP)+1), 'hex')

  return {
    peer: peer,
    seq: seq
  }
}

var getSeqs = function(head) {
  var seqs = {}
  for (var i = 0; i < head.length; i++) seqs[head[i].peer] = head[i].seq
  return seqs
}

var Log = function(db, opts) {
  if (!(this instanceof Log)) return new Log(db, opts)
  if (!opts) opts = {}

  events.EventEmitter.call(this)

  this.head = null
  this.db = db
  this.id = null
  this.corked = 1

  this._encoding = opts.valueEncoding || 'binary'
  this._onflush = this._onflush.bind(this)
  this._batch = []
  this._waiting = []
  this._writing = null
  this._indices = {}

  var self = this

  load(opts.id, db, function(err, id, head) {
    if (err) return self.emit('error', err)

    self.id = id
    self.head = head
    for (var i = 0; i < head.length; i++) self._indices[head[i].peer] = i

    self.emit('ready')
    self.uncork()
  })
}

util.inherits(Log, events.EventEmitter)

Log.prototype.cork = function() {
  this.corked++
}

Log.prototype.uncork = function() {
  if (this.corked) this.corked--
  if (this.corked) return
  while (this._waiting.length) this._waiting.shift()()
}

Log.prototype.append = function(entry, cb) {
  if (this.corked) return this._wait(this.append, arguments, false)

  var me = this._getPeer(this.id, 0)

  this._write({
    peer: this.id,
    seq: me.seq+1,
    entry: entry
  }, cb)
}

Log.prototype.createAppendStream = function() {
  var self = this
  return through.obj(function(data, enc, cb) {
    self.append(data, cb)
  })
}

Log.prototype.createReplicationStream = function() {
  if (this.corked) return this._wait(this.createReplicationStream, arguments, true)

  var self = this
  var handshake = null
  var seqs = null

  var pack = lpstream.encode()
  var unpack = lpstream.decode()
  var result = duplexify(unpack, pack)

  var ondata = function(data, cb) {
    seqs[data.peer] = data.seq
    self._write(data, cb)
  }

  var onhandshake = function(data, cb) {
    if (result.destroyed) return cb()

    handshake = data
    seqs = getSeqs(handshake.head)

    result.id = handshake.id
    result.emit('connect', handshake.id, handshake.peers)

    var map = function(change) {
      if (seqs[change.peer] >= change.seq) return null // already has this change
      return messages.Change.encode(change)
    }

    var rs = self.createReadStream({
      live: true,
      valueEncoding: 'binary',
      since: handshake.head,
      map: map
    })

    pump(rs, pack)
    cb()
  }

  var ws = through(function(data, enc, cb) {
    if (handshake) ondata(messages.Change.decode(data), cb)
    else onhandshake(messages.Handshake.decode(data), cb)
  })

  pump(unpack, ws)
  pack.write(messages.Handshake.encode(this))

  return result
}

Log.prototype.createWriteStream = function() {
  if (this.corked) return this._wait(this.createWriteStream, arguments, true)

  var self = this
  return through.obj(function(change, enc, cb) {
    self._write(change, cb)
  })
}

Log.prototype.createReadStream = function(opts) {
  if (this.corked) return this._wait(this.createReadStream, arguments, true)

  if (!opts) opts = {}

  opts.entry = opts.entry !== false
  opts.head = !!opts.head
  opts.valueEncoding = opts.valueEncoding || this._encoding
  opts.map = opts.map || echo // mostly for internal usage

  if (opts.tail) return this._tail(this._toHead(this.head), opts)

  var self = this
  var head = this._toHead(opts.since)

  var streams = head.map(function(h) {
    return function() {
      var rs = self.db.createReadStream({
        start: encodeKey(h.peer, h.seq+1),
        end: encodeKey(h.peer, -1),
        valueEncoding: opts.valueEncoding
      })

      var track = through.obj(function(data, enc, cb) {
        var entry = data.value
        var change = decodeKey(data.key)

        h.seq = change.seq

        if (opts.entry) change.entry = entry
        if (opts.head) change.head = head

        cb(null, opts.map(change))
      })

      return pump(rs, track)
    }
  })

  if (opts.live) {
    streams.push(function() {
      return self._tail(head, opts)
    })
  }

  return multistream.obj(streams)
}

Log.prototype._toHead = function(since) {
  if (!since) since = []

  var seqs = getSeqs(since)
  var result = []
  for (var i = 0; i < this.head.length; i++) {
    var h = this.head[i]
    result[i] = {peer:h.peer, seq: seqs[h.peer] === undefined ? 0 : seqs[h.peer]}
  }

  return result
}

Log.prototype._tail = function(head, opts) {
  var self = this
  var waiting = null

  var onflush = function() {
    if (!waiting) return
    var cb = waiting
    waiting = null
    read(0, cb)
  }

  var select = function() {
    for (var i = 0; i < self.head.length; i++) {
      if (i >= head.length) head.push({peer:self.head[i].peer, seq:0})
      if (head[i].seq < self.head[i].flushed) return head[i]
    }
    return null
  }

  var read = function(size, cb) {
    var h = select()

    if (!h) {
      waiting = cb
      return
    }

    self.db.get(encodeKey(h.peer, h.seq+1), {valueEncoding:opts.valueEncoding}, function(err, data) {
      if (err) return cb(err)

      h.seq++

      var change = {
        peer: h.peer,
        seq: h.seq
      }

      if (opts.entry) change.entry = data
      if (opts.head) change.head = head

      change = opts.map(change)
      if (!change) return read(size, cb)

      cb(null, change)
    })
  }

  var tail = from.obj(read)

  this.on('flush', onflush)
  tail.on('close', function() {
    self.removeListener('flush', onflush)
  })

  return tail
}

Log.prototype._write = function(change, cb) {
  if (!cb) cb = noop

  var peer = this._getPeer(change.peer, change.seq-1)

  if (peer.seq >= change.seq) return cb() // return early

  peer.seq = change.seq

  this.emit('append', change)

  this._batch.push({
    type: 'put',
    key: encodeKey(change.peer, change.seq),
    value: change.entry,
    valueEncoding: this._encoding,
    seq: change.seq,
    peer: peer,
    callback: cb
  })

  this._flush()
}

Log.prototype._flush = function() {
  if (this._writing || !this._batch.length) return
  this._writing = this._batch
  this._batch = []
  this.db.batch(this._writing, this._onflush)
}

Log.prototype._onflush = function(err) {
  if (err) return this.emit('error', err)

  var writing = this._writing
  this._writing = null

  for (var i = 0; i < writing.length; i++) {
    var w = writing[i]
    w.peer.flushed = w.seq
    w.callback()
  }

  this.emit('flush', writing)
  this._flush()
}

Log.prototype._wait = function(fn, args, isStream) {
  var self = this

  if (isStream) {
    var proxy = duplexify.obj()

    this._waiting.push(function() {
      if (proxy.destroyed) return
      var s = fn.apply(self, args)
      proxy.setWritable(s.writable ? s : false)
      proxy.setReadable(s.readable ? s : false)
    })

    return proxy
  } else {
    this._waiting.push(function() {
      fn.apply(self, args)
    })
  }
}

Log.prototype._getPeer = function(peer, seq) {
  if (this._indices[peer] !== undefined) return this.head[this._indices[peer]]
  var i = this.head.push({peer:peer, seq:seq, flushed:0})-1
  this._indices[peer] = i
  return this.head[i]
}

module.exports = Log