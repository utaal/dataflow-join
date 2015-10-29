extern crate mmap;
extern crate time;
extern crate timely;
extern crate dataflow_join;

use std::rc::Rc;

use dataflow_join::*;
use dataflow_join::graph::{GraphTrait, GraphMMap, GraphExtenderExt};

use timely::dataflow::*;
use timely::dataflow::operators::*;

// use timely::communication::Communicator;

fn main () {

    let filename = std::env::args().nth(1).unwrap();

    let graph = Rc::new(GraphMMap::<u32>::new(&filename));
    println!("{}", graph.nodes());
}
