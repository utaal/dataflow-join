//! Extension methods for `Stream` based on record-by-record transformation.

extern crate timely;

use std::slice::Iter;
use std::ops::RangeFull;
use std::fmt::Debug;
use std::collections::LinkedList;

use timely::dataflow::channels::Content;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::unary::Unary;
use timely::drain::{Drain, DrainExt};
use timely::Data;

/// Extension trait for `Stream`.
pub trait ChunkedMap<S: Scope, D: Data> {
    /// Consumes each element of the stream and yields some number of new elements.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .flat_map(|x| (0..x))
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn chunked_flat_map<I: Iterator, L: Fn(D)->I+'static>(&self, logic: L) -> Stream<S, I::Item> where I::Item: Data, D: Debug;
}

impl<S: Scope, D: Data> ChunkedMap<S, D> for Stream<S, D> {
    fn chunked_flat_map<I: Iterator, L: Fn(D)->I+'static>(&self, logic: L) -> Stream<S, I::Item>
    where I::Item: Data, D: Debug {
        let mut stash: Option<(S::Timestamp, Vec<D>)> = None;
        self.unary_stream(Pipeline, "ChunkedFlatMap", move |input, output| {
            match stash.clone() {
                Some((time, ref mut data)) => {
                    let mut drain = data.drain_temp();
                    if let Some(x) = drain.next() {
                        output.session(&time).give_iterator(logic(x));
                    }
                },
                None => if let Some((time, data)) = input.next() {
                    let mut vec = Vec::new();
                    for datum in data.drain_temp() { vec.push(datum); }
                    stash = Some(((*time).clone(), vec));
                }
            }
        })
    }
}
