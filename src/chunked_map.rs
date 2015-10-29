//! Extension methods for `Stream` based on record-by-record transformation.

extern crate timely;

use std::slice::Iter;
use std::ops::RangeFull;
use std::fmt::Debug;
use std::collections::LinkedList;
use std::iter::FromIterator;

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
    fn chunked_flat_map<I: Iterator, L: Fn(D)->I+'static>(&self, logic: L) -> Stream<S, I::Item> where I::Item: Data, D: Debug, I::Item: Debug;
}

impl<S: Scope, D: Data> ChunkedMap<S, D> for Stream<S, D> {
    fn chunked_flat_map<I: Iterator, L: Fn(D)->I+'static>(&self, logic: L) -> Stream<S, I::Item>
    where I::Item: Data, D: Debug, I::Item: Debug {
        let mut stashed_time: Option<S::Timestamp> = None;
        let mut in_data = LinkedList::<D>::new();
        let mut out_data = LinkedList::<I::Item>::new();
        self.unary_stream(Pipeline, "ChunkedFlatMap", move |input, output| {
            for _ in 0..100 {
                match stashed_time {
                    Some(t) => {
                        let out_datum: Option<I::Item> = if let Some(datum) = out_data.pop_front() {
                            Some(datum)
                        } else if let Some(x) = in_data.pop_front() {
                            out_data = logic(x).collect::<LinkedList<I::Item>>();
                            out_data.pop_front()
                        } else {
                            stashed_time = None;
                            break;
                            None
                        };
                        if let Some(datum) = out_datum {
                            output.session(&t).give(datum);
                        }
                    },
                    None => if let Some((time, data)) = input.next() {
                        in_data = data.drain_temp().collect::<LinkedList<D>>();
                        stashed_time = Some((*time).clone());
                    }
                }
            }
        })
    }
}
