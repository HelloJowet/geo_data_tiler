use std::collections::HashMap;

use geohashrust::{BinaryHash, GeoLocation};
use polars::prelude::*;

pub struct Tiler {
    pub binary_hash_precision: u8,
    pub max_allowed_features_in_binary_hash: u64,
    pub binary_hash_count: HashMap<String, i64>,
}

impl Tiler {
    pub fn new(binary_hash_precision: u8, max_allowed_features_in_binary_hash: u64) -> Self {
        Tiler {
            binary_hash_precision: binary_hash_precision,
            max_allowed_features_in_binary_hash: max_allowed_features_in_binary_hash,
            binary_hash_count: HashMap::new(),
        }
    }

    pub fn add_coordinate(&mut self, latitude: f64, longitude: f64) {
        let geometry = GeoLocation {
            latitude: latitude,
            longitude: longitude,
        };
        let binary_hash = BinaryHash::encode(&geometry, self.binary_hash_precision).to_string();
        *self.binary_hash_count.entry(binary_hash).or_insert(0) += 1;
    }

    pub fn get_tiles(&self) -> Result<(), PolarsError> {
        let node_count: Vec<i64> = self.binary_hash_count.clone().into_values().collect();
        let binary_hash: Vec<String> = self.binary_hash_count.clone().into_keys().collect();

        let mut binary_hash_count_df = df!(
            "node_count" => node_count,
            "binary_hash" => binary_hash
        )?;

        let mut binary_hash_results = HashMap::new();

        for i in 0..self.binary_hash_precision as usize {
            let sliced_binary_hash: Vec<&str> = binary_hash_count_df
                .column("binary_hash")?
                .utf8()?
                .into_no_null_iter()
                .map(|binary_hash_value: &str| &binary_hash_value[..i + 1])
                .collect();
            let temp_binary_hash_count_df = binary_hash_count_df
                .with_column(Series::new("sliced_binary_hash", sliced_binary_hash))?
                .clone();

            let grouped_binary_hash_df = temp_binary_hash_count_df
                .lazy()
                .group_by([col("sliced_binary_hash")])
                .agg([
                    col("node_count").sum().alias("total_node_count"),
                    col("binary_hash").reverse().alias("binary_hashes"),
                ])
                .collect()?;
            let binary_hashes_over_max_allowed_features_df = grouped_binary_hash_df
                .clone()
                .lazy()
                .filter(col("total_node_count").gt(lit(self.max_allowed_features_in_binary_hash)))
                .collect()?
                .explode(["binary_hashes"])?
                .rename("binary_hashes", "binary_hash")?
                .drop_many(&["sliced_binary_hash", "total_node_count"])
                .left_join(&binary_hash_count_df, ["binary_hash"], ["binary_hash"])?;
            binary_hash_count_df = binary_hashes_over_max_allowed_features_df;

            let binary_hashes_under_max_allowed_features_df = grouped_binary_hash_df
                .lazy()
                .filter(
                    col("total_node_count").lt(lit(self.max_allowed_features_in_binary_hash + 1)),
                )
                .collect()?;
            let sliced_binary_hash_list: Vec<String> = binary_hashes_under_max_allowed_features_df
                .column("sliced_binary_hash")?
                .utf8()?
                .into_no_null_iter()
                .map(|geohash| geohash.to_string())
                .collect();
            let node_count_list: Vec<i64> = binary_hashes_under_max_allowed_features_df
                .column("total_node_count")?
                .i64()?
                .into_no_null_iter()
                .collect();

            for (node_count, sliced_binary_hash) in node_count_list
                .into_iter()
                .zip(sliced_binary_hash_list.into_iter())
            {
                binary_hash_results.insert(sliced_binary_hash, node_count);
            }
        }

        let binary_hash_list: Vec<String> = binary_hash_count_df
            .column("binary_hash")?
            .utf8()?
            .into_no_null_iter()
            .map(|geohash| geohash.to_string())
            .collect();
        let node_count_list: Vec<i64> = binary_hash_count_df
            .column("node_count")?
            .i64()?
            .into_no_null_iter()
            .collect();

        for (node_count, binary_hash) in node_count_list
            .into_iter()
            .zip(binary_hash_list.into_iter())
        {
            binary_hash_results.insert(binary_hash, node_count);
        }

        println!("{:?}", binary_hash_results);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::tiler::Tiler;

    #[test]
    fn it_works() {
        let mut tiler = Tiler::new(11, 100000000);

        tiler.add_coordinate(1.0, 1.0);
        tiler.add_coordinate(1.0, 2.0);
        tiler.add_coordinate(2.0, 3.0);
        tiler.add_coordinate(4.0, 1.0);
        tiler.add_coordinate(1.5, 1.5);

        tiler.get_tiles().unwrap();

        println!("lol");
        assert_eq!(4, 4);
    }
}
