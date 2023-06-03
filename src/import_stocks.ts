import axios from "axios";
import oracledb = require("oracledb");
process.env.ORA_SDTZ = "UTC";
import { existsSync, promises } from "fs";

class Script {
  paramsFilePath: string = "./params.json";
  // julebApiUrl = "https://api.juleb.com/agent_receiver/retailpro";
  julebApiUrl = "https://7f4d-176-18-80-157.ngrok-free.app/retailpro";

  constructor() {}
  async fetchStoreName(connection, store_code, identifier) {
    let sql = ``;
    let binds = {};
    if (identifier === "store_code") {
      sql = `SELECT CMS.STORE.STORE_NAME,
      CMS.STORE.STORE_CODE,
      CMS.STORE.STORE_NO
      FROM CMS.STORE
      WHERE CMS.STORE.STORE_CODE = :storeCode`;
      binds = {
        storecode: {
          dir: oracledb.BIND_IN,
          val: store_code,
          type: oracledb.STRING,
        },
      };
    } else if (identifier === "store_no") {
      sql = `SELECT CMS.STORE.STORE_NAME,
      CMS.STORE.STORE_CODE,
      CMS.STORE.STORE_NO
      FROM CMS.STORE
      WHERE CMS.STORE.STORE_NO = :storeCode`;
      binds = {
        storecode: {
          dir: oracledb.BIND_IN,
          val: parseInt(store_code),
          type: oracledb.NUMBER,
        },
      };
    }
    try {
      const options = {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
      };
      const slipsQuery = await connection.execute(sql, binds, options);
      return {
        store_name: slipsQuery.rows[0].STORE_NAME,
        store_no: slipsQuery.rows[0].STORE_NO,
        store_code: slipsQuery.rows[0].STORE_CODE,
      };
    } catch (err) {
      console.log(err);
    }
  }

  async pollBranchStocks(connection, stores) {
    for (let i = 0; i < stores.length; i++) {
      const currentStore = stores[i];
      try {
        let sql;
        let binds = [];
        let store_info = await this.fetchStoreName(
          connection,
          currentStore,
          "store_code"
        );
        sql = `SELECT CMS.LOT_QTY.LOT_NUMBER,
                      CMS.LOT_QTY.QTY,
                      CMS.LOT_QTY.ITEM_SID,
                      CMS.LOT.EXPIRY_DATE,
                      CMS.INVENTORY_ALU_ALL.ALU,
                      CMS.INVN_SBS.ACTIVE
                      FROM CMS.LOT_QTY
                      LEFT JOIN CMS.LOT
                      ON CMS.LOT_QTY.LOT_NUMBER = CMS.LOT.LOT_NUMBER
                      LEFT JOIN CMS.INVN_SBS
                      ON CMS.LOT_QTY.ITEM_SID = INVN_SBS.ITEM_SID
                      LEFT JOIN CMS.INVENTORY_ALU_ALL
                      ON CMS.LOT_QTY.ITEM_SID = CMS.INVENTORY_ALU_ALL.ITEM_SID
                      WHERE CMS.LOT_QTY.QTY != 0 AND INVN_SBS.ACTIVE = 1 AND CMS.LOT_QTY.STORE_NO = :v1`;
        binds = [store_info.store_no];
        const options = {
          outFormat: oracledb.OUT_FORMAT_OBJECT,
          fetchInfo: {
            ITEM_SID: { type: oracledb.STRING },
            LOT_NUMBER: { type: oracledb.STRING },
          },
        };
        console.log("fetching query");
        const stocksQuery = await connection.execute(sql, binds, options);
        console.log(stocksQuery.rows);
        const stockPayload = {
          store_code: store_info.store_code,
          lines: stocksQuery.rows,
        };

        let summary = {
          number_of_orders: stockPayload.lines.length,
          receiving_store: store_info.store_code,
        };

        await axios
          .post(`${this.julebApiUrl}/import-stock`, stockPayload)
          .then(async function (response) {
            console.log("sent:");
            console.log(summary);
            console.log(response);
          });
      } catch (error) {
        console.log(error);
      }
    }
  }

  async wrapper() {
    const paramsExist = existsSync(this.paramsFilePath);
    if (!paramsExist) {
      console.log(
        "make sure to have params.json and bookmark.json in the project directory"
      );
      return;
    }
    const params: IParams = JSON.parse(
      (await promises.readFile(this.paramsFilePath)).toString()
    );
    console.log({ params });
    const stores =
      typeof params.storeCode === "string"
        ? params.storeCode.split(",")
        : params.storeCode;
    const connection = await oracledb.getConnection({
      user: params.user ? params.user : "reportuser",
      password: params.password ? params.password : "report",
      connectString: params.connectString
        ? params.connectString
        : "localhost:1521/rproods",
    });
    this.pollBranchStocks(connection, stores);
  }
}

const script = new Script();
script.wrapper();

interface IParams {
  user: string;
  password: string;
  connectString: string;
  storeCode: any;
}
