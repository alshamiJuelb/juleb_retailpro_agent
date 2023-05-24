import axios from "axios";
import oracledb = require("oracledb");

import { existsSync, promises } from "fs";

class Script {
  paramsFilePath: string = "./params.json";
  julebApiUrl = "https://api.juleb.com/agent_receiver/retailpro";
  constructor() {}

  async syncProducts(connection) {
    const currDate = new Date();
    const offset = 60 * 60 * 24 * 1000 * 2;
    currDate.setTime(currDate.getTime() - offset);
    const formattedDate = currDate.toISOString().substring(0, 10);
    let sql;
    let binds = [];
    sql = `SELECT CMS.INVN_SBS.ALU,
          CMS.INVN_SBS.DCS_CODE,
          DESCRIPTION1,
          DESCRIPTION2,
          DESCRIPTION3,
          VEND_CODE,
          CMS.INVN_SBS.TAX_CODE,
          CMS.INVN_SBS.CMS_POST_DATE,
          CMS.INVN_SBS.MODIFIED_DATE,
          CMS.INVN_SBS.ITEM_SID,
          CMS.INVN_SBS.COST,
          CMS.INVN_SBS_PRICE.ITEM_SID,
          CMS.INVN_SBS_PRICE.PRICE,
          CMS.INVN_SBS_PRICE.PRICE_LVL,
          D_NAME,
          C_NAME,
          S_NAME
          FROM CMS.INVN_SBS
          LEFT JOIN CMS.INVN_SBS_PRICE
          ON CMS.INVN_SBS.ITEM_SID = CMS.INVN_SBS_PRICE.ITEM_SID
          LEFT JOIN CMS.DCS
          ON CMS.DCS.DCS_CODE = CMS.INVN_SBS.DCS_CODE
          WHERE CMS.INVN_SBS.VEND_CODE = '888888'`; //
    const options = {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
      fetchInfo: {
        ITEM_SID: { type: oracledb.STRING },
      },
    };
    console.log("fetching masterdata query");
    const masterDataQuery = await connection.execute(sql, binds, options);
    const payload = {
      selector: "Full",
      lines: masterDataQuery.rows,
    };
    await axios
      .post(`${this.julebApiUrl}/master-data`, payload)
      .then(async function (response) {
        console.log("sent:");
        console.log(masterDataQuery.rows.length);
        console.log(response);
      });
  }

  async syncPrices(connection) {
    const currDate = new Date();
    const offset = 60 * 60 * 3 * 1000;
    currDate.setTime(currDate.getTime() - offset);
    let sql;
    let binds = [];
    sql = `SELECT CMS.INVN_SBS.ALU,
    CMS.INVN_SBS.TAX_CODE,
    CMS.INVN_SBS.ITEM_SID,
    CMS.INVN_SBS.COST,
    CMS.INVN_SBS.SBS_NO,
    CMS.INVN_SBS_PRICE.ITEM_SID,
    CMS.INVN_SBS_PRICE.PRICE,
    CMS.INVN_SBS_PRICE.PRICE_LVL
    FROM CMS.INVN_SBS
    LEFT JOIN CMS.INVN_SBS_PRICE
    ON CMS.INVN_SBS.ITEM_SID = CMS.INVN_SBS_PRICE.ITEM_SID
    WHERE CMS.INVN_SBS_PRICE.PRICE_LVL = 1
    AND CMS.INVN_SBS.SBS_NO = 8`;
    const options = {
      outFormat: oracledb.OUT_FORMAT_OBJECT,
      fetchInfo: {
        ITEM_SID: { type: oracledb.STRING },
      },
    };
    console.log("fetching prices query");
    const masterDataQuery = await connection.execute(sql, binds, options);
    const payload = {
      selector: "Prices",
      lines: masterDataQuery.rows,
    };

    console.log({
      selector: payload.selector,
      lines: payload.lines.slice(0, 10),
    });

    await axios
      .post(`${this.julebApiUrl}/master-data`, payload)
      .then(async function (response) {
        console.log("sent:");
        console.log(masterDataQuery.rows);
        console.log(response);
      });
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
    const connection = await oracledb.getConnection({
      user: params.user ? params.user : "reportuser",
      password: params.password ? params.password : "report",
      connectString: params.connectString
        ? params.connectString
        : "localhost:1521/rproods",
    });
    await this.syncProducts(connection);
    // await this.syncPrices(connection);
  }
}

const script = new Script();
script.wrapper();

interface IParams {
  user: string;
  password: string;
  connectString: string;
}
