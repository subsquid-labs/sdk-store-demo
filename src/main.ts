import { TypeormDatabase } from "@subsquid/typeorm-store";
import { Burn } from "./model";
import { processor, Block } from "./processor";
import { S3Dest } from "@subsquid/file-store-s3";
import { Dest, LocalDest } from "@subsquid/file-store";
import { assertNotNull } from "@subsquid/util-internal";
import * as dotenv from "dotenv";
import { Database } from "@subsquid/file-store";
import {
  Column,
  Table,
  Compression,
  Types,
} from "@subsquid/file-store-parquet";
const dbOptions = {
  tables: {
    BurnsTable: new Table(
      "burns.parquet",
      {
        id: Column(Types.String()),
        block: Column(Types.String()),
        address: Column(Types.String()),
        value: Column(Types.String()),
        tx_hash: Column(Types.String()),
      },
      {
        compression: "GZIP",
        rowGroupSize: 300000,
        pageSize: 1000,
      }
    ),
  },
  dest: new LocalDest("./data"),
  chunkSizeMb: 10,
};

processor.run(new Database(dbOptions), async (ctx) => {
  let burns = [];
  for (let c of ctx.blocks) {
    for (let tx of c.transactions) {
      // decode and normalize the tx data
      let id = tx.id;
      let block = c.header.height.toString();
      let address = tx.from;
      let value = tx.value.toString();
      let tx_hash = tx.hash;
      burns.push({ id, block, address, value, tx_hash });
    }
  }

  // upsert batches of entities with batch-optimized ctx.store.save
  await ctx.store.BurnsTable.writeMany(burns);
});
