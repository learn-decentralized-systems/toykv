#   Toy Key-Value

This library provides an easy to use interface to [PebbleDB][p].
The original interface is designed for high performance, so, for
example, it returns slices of internal slabs. It also provides
lots of knobs to optimize inner wokings of the databale. That is
not necessary in 80% of cases, but adds plenty of boilerplate.
ToyKV hides that behind a slim API. The raw interface is still
available though, see `kv.DB`.

ToyKV also imposes basic store orgatization: the type of a
record is specified by one letter, the key is any `Stringer`
(e.g. time or IP address), the value is a string. The key
difference of golang's []byte and string is immutability, so
unless you are concerned about performance, you take the safe
path and use `string`.

ToyKV supports two consistency levels: sync and best effort. The
sync mode employs WAL and waits each commit to be reliably
stored. The best effort mode (no WAL, no wait) may forget recent
writes in case of a crash. To run in the sync mode, use `".db"`
extension (for "database"), e.g. `Open("name.db")`. For the best
effort mode, name the dir in some other way.

See the tests for API usage examples.

[p]: https://github.com/cockroachdb/pebble
