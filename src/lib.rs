mod bucket;
mod map;

pub use crate::map::HashMap;

pub fn gen_assembly() {
    let g = crossbeam::epoch::pin();
    let m = HashMap::<u64, u64>::with_pow_buckets(2);
    m.get(&0, &g);
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
