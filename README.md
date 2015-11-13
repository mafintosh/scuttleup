# scuttleup

Scuttlebutt like eventual consistent log replication for levelup

```
npm install scuttleup
```

[![build status](http://img.shields.io/travis/mafintosh/scuttleup.svg?style=flat)](http://travis-ci.org/mafintosh/scuttleup)

## Usage

``` js
var scuttleup = require('scuttleup')

var log = scuttleup(db) // db is a levelup instance

var changes = log2.createReadStream({
  live: true
})

changes.on('data', function(data) {
  console.log(data) // print out the log - data.entry will be 'hello world'
})

log.append('hello world') // add something to the log
```

## Replication

To replicate two logs pipe their replication stream together using the [scuttlebutt protocol](http://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf)

``` js
var repl1 = log1.createReplicationStream()
var repl2 = log2.createReplicationStream()

// the two logs will now replicate to each other
repl1.pipe(repl2).pipe(repl1)
```

## API

#### `var log = scuttleup(db, [opts])`

Create a log new instance. Options can include

``` js
{
  id: 'a-globally-unique-peer-id',
  valueEncoding: 'utf-8' // encoding of log entries
}
```

#### `log.append(entry, [callback])`

Add a new entry to the log

#### `log.entry(peer, seq, [options], callback)`

Retrieve a entry from the log from a given `peer` and `seq`

#### `var ws = log.createAppendStream()`

`.append` as a stream

#### `var repl = log.createReplicationStream(opts)`

Create a log replication stream. Pipe this to the replication stream of another log.
Replication is eventual consistent and works using the [scuttlebutt protocol](http://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf)

Options can include

``` js
{
  live: false, // disable live replication. defaults to true
  mode: 'sync' | 'push' | 'pull' // set replication mode. defaults to sync
}
```

#### `var rs = log.createReadStream(opts)`

Create a log read stream. Options can include

``` js
{
  live: false, // continiously read the changes,
  tail: false, // only read new changes
}
```

#### `var ws = log.createWriteStream()`

Create a log write stream

## License

MIT
