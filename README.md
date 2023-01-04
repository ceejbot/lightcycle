# LightCycle

This is a translation into Rust of a [toy consistent hash ring](https://github.com/ceejbot/light-cycle) I wrote in Javascript a long time ago. I'm not sure it's useful for any production workloads, but the concept is generally handy for distributing items to to specific instances of otherwise-identical resources in a balanced way. An example might be sticky sessions, or items you want to cache among a number of redis shards.

It turned out to be quite trivial to implement, and is simpler than the JS implementation. For one thing, the error case handling is clearer and more consistent in fewer lines of code, thanks to compile-time guarantees. It was also an excuse to use a [GAT](https://doc.rust-lang.org/rust-by-example/generics/assoc_items/types.html) for the first time.

## Example

Here's the redis example from the js version, roughly translated. Error handling is a bit sketch here.

```rust
use lightcycle::{HasId, LightCycle};
use redis::Commands;

#[derive(Debug, Clone)]
struct RedisCacheShard {
	client: redis::Client
	uri: String
}

impl RedisCacheShard {
	fn new(host, port, db) -> anyhow::Result<Self>{
		let uri = format!("redis://{host}:{port}/{db}");
		let client = redis::Client::open(uri)?;
		Ok(Self { client, uri })
	}
}

impl HasId for RedisCacheShard {
	fn id(&self) -> &str {
		// We choose the redis URI as a reasonable unique id for each shard.
		&self.uri
	}
}

fn some_function(shards: Vec<RedisCacheShard>) -> anyhow::Result<()> {
	// Create entries in the hash ring for each of our redis caches,
	let mut ring = LightCycle::new_with_replica_count(10);
	for cache in shards {
		ring.add(Box::new(shard));
	}

	...

	// now find a home for some data we need to cache
	let id = "some text string id for data we need to store somewhere".to_string();
	let shard = ring.locate(id);
	shard.client.connection()?.set(id, myDataSerializedSomehow)?;

	...
}
```

## API

`LightCycle::new()` creates a new consistent hash ring with a replica count of 4.

`LightCycle::new_with_replica_count(usize)` creates one with the replica count you want. No attempt is made to put sensible bounds on this number; it really should be a smaller int to force some reality.

Implement the trait `HasId` on anything you want to store in a light cycle. `lightcycle.add(Box::new(thing))` stores one of your thingies. `lightcycle.remove(id_string)` removes it.

`lightcycle.locate("some string here")` finds the right home for something you want to distribute among your resources.

`cargo doc --open` has additional details.

## LICENSE

[Blue Oak Model License](https://blueoakcouncil.org/license/1.0.0); text in [LICENSE.md](./LICENSE.md).
