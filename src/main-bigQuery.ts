import { TypeormDatabase } from "@subsquid/typeorm-store";
import { Burn } from "./model";
import { processor, Block } from "./processor";
import { S3Dest } from "@subsquid/file-store-s3";
import { Dest, LocalDest } from "@subsquid/file-store";
import { assertNotNull } from "@subsquid/util-internal";
import * as dotenv from "dotenv";
import { Column, Table, Types, Database } from "@subsquid/bigquery-store";
import { BigQuery } from "@google-cloud/bigquery";

const db = new Database({
  bq: new BigQuery(),
  dataset: "subsquid-datasets.test_dataset",
  tables: {
    BurnsTable: new Table("burns", {
      id: Column(Types.String()),
      block: Column(Types.BigNumeric(38)),
      address: Column(Types.String()),
      value: Column(Types.BigNumeric(38)),
      tx_hash: Column(Types.String()),
    }),
  },
});
processor.run(db, async (ctx) => {
  let burns = [];
  for (let c of ctx.blocks) {
    for (let tx of c.transactions) {
      // decode and normalize the tx data
      let id = tx.id;
      let block = c.header.height;
      let address = tx.from;
      let value = tx.value;
      let tx_hash = tx.hash;
      burns.push({ id, block, address, value, tx_hash });
    }
  }

  // upsert batches of entities with batch-optimized ctx.store.save
  await ctx.store.BurnsTable.insertMany(burns);
});
