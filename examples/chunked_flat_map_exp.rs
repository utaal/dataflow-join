extern crate mmap;
extern crate time;
extern crate timely;
extern crate dataflow_join;

use std::rc::Rc;

use dataflow_join::*;
use dataflow_join::graph::{GraphTrait, GraphMMap, GraphExtenderExt};
use dataflow_join::chunked_map::ChunkedMap;

use timely::dataflow::*;
use timely::dataflow::operators::*;

// use timely::communication::Communicator;

fn main () {

    timely::execute_from_args(std::env::args().skip(4), move |root| {

        let mut input = root.scoped(|builder| {

            let (input, stream) = builder.new_input::<u32>();

            let flatmapped = stream.chunked_flat_map(|i| vec![(i, i + 1), (i, i + 2), (i, i + 3)].into_iter());

            flatmapped.inspect(|&(a, b)| println!("{},{}", a, b));

            input
        });


        for i in 0..100 {
            input.send(i);
            if (i % 10 == 9) {
                input.advance_to((i / 10) as u64 + 1);
            }
            root.step();
        }

        input.close();
        while root.step() { }
    })
}
