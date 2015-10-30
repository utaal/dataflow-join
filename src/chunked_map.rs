//! Extension methods for `Stream` based on record-by-record transformation.

extern crate timely;

use std::slice::Iter;
use std::ops::RangeFull;
use std::fmt::Debug;
use std::collections::LinkedList;
use std::iter::FromIterator;
use std::cell::RefCell;


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
    fn chunked_flat_map<I: Iterator, L: Fn(D)->I+'static>(&self, logic: L) -> Stream<S, I::Item> where I::Item: Data, D: Debug, I::Item: Debug, I: 'static;
}

impl<S: Scope, D: Data> ChunkedMap<S, D> for Stream<S, D> {
    fn chunked_flat_map<I: Iterator, L: Fn(D)->I+'static>(&self, logic: L) -> Stream<S, I::Item>
    where I::Item: Data, D: Debug, I::Item: Debug, I: 'static {
        let mut stashed: Box<Option<(S::Timestamp, LinkedList<I>)>> = Box::new(None);
        self.unary_stream(Pipeline, "ChunkedFlatMap", move |input, output| {
            let mut remaining = 100;
            while remaining > 0 {
                if let None = *stashed {
                    *stashed = input.next().and_then(|(time, data)| {
                        let mut iterators: LinkedList<I> = data.drain_temp().map(|x| logic(x)).collect();
                        Some(((*time).clone(), iterators))
                    });
                    if let None = *stashed {
                        return;
                    }
                };
                let mut stashed_valid: bool = true;
                if let Some((time, ref mut iterators)) = *stashed {
                    if let Some(mut it) = iterators.pop_front() {
                        if let Some(datum) = it.next() {
                            output.session(&time).give(datum);
                            iterators.push_front(it);
                            remaining -= 1;
                        };
                    } else {
                        stashed_valid = false;
                    };
                };
                if !stashed_valid {
                    *stashed = None;
                }
            }
        })
    }
}
