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

    let filename = std::env::args().nth(1).unwrap();
    let inspect = std::env::args().nth(2).unwrap() == "inspect";
    let step_size = std::env::args().nth(3).unwrap().parse::<usize>().unwrap();

    timely::execute_from_args(std::env::args().skip(4), move |root| {

        let index = root.index();
        let peers = root.peers();

        let graph = Rc::new(GraphMMap::<u32>::new(&filename));
        println!("{}", graph.nodes());

        let mut input = root.scoped(|builder| {

            let (input, stream) = builder.new_input::<u32>();

            // extend u32s to pairs, then pairs to triples.
            let triangles = stream.extend(vec![&graph.extend_using(|&a| a as u64)])
                                  .chunked_flat_map(|(p, es)| es.into_iter().map(move |e| (p, e)))
                                  .extend(vec![&graph.extend_using(|&(a,_)| a as u64),
                                               &graph.extend_using(|&(_,b)| b as u64)])
                                  .chunked_flat_map(|((a, b), cs)| cs.into_iter().map(move |c| (a, b, c)));

            // // Quads
            // triangles.flat_map(|(p,es)| es.into_iter().map(move |e| (p, e)))
            //          .extend(vec![&graph.extend_using(|&((a,_),_)| a as u64),
            //                       &graph.extend_using(|&((_,b),_)| b as u64),
            //                       &graph.extend_using(|&((_,_),c)| c as u64)]);

            if inspect { triangles.inspect(|&(a, b, c)| println!("{},{},{}", a, b, c)); }

            input
        });

        let nodes = graph.nodes() - 1;
        let limit = (nodes / step_size) + 1;
        for round in (0..limit) {
            for source in 0..step_size {
                let candidate = source + round * step_size;
                if candidate % peers == index && candidate < nodes {
                    input.send(candidate as u32);
                }
            }

            // input.send_at(round, (0..step_size).map(|x| x + round * step_size)
            //                                    .filter(|&x| x % peers == index)
            //                                    .filter(|&x| x < nodes)
            //                                    .map(|x| x as u32));

            input.advance_to(round as u64 + 1);
            root.step();
        }

        input.close();
        while root.step() { }
    })
}
