import axios from "axios";
import oracledb = require("oracledb");

import { existsSync, promises } from "fs";

class Script {
  paramsFilePath: string = "./params.json";
  bookmarkFilePath: string = "./bookmarks.json";
  constructor() {}

  async fetchStoreName(connection, store_code, identifier) {
    let sql = ``;
    let binds = {};
    if (identifier === "store_code") {
      sql = `SELECT CMS.STORE.STORE_NAME,
    CMS.STORE.STORE_CODE,
    CMS.STORE.STORE_NO,
    CMS.STORE.SBS_NO
    FROM CMS.STORE
    WHERE CMS.STORE.STORE_CODE = :storeCode
    AND CMS.STORE.SBS_NO = 8`;
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

  async minifetchSlip(connection, slip_sid) {
    const sql2 = `SELECT CMS.SLIP.SLIP_SID,
            
            CMS.SLIP.SLIP_NO,
            CMS.SLIP.MODIFIED_DATE,
            CMS.SLIP.CREATED_DATE,
            CMS.SLIP.POST_DATE
            FROM CMS.SLIP
            WHERE CMS.SLIP.SLIP_SID = `;
    let binds = {};
    let sql = sql2.concat(slip_sid);
    try {
      const options = {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
        fetchInfo: {
          SLIP_SID: { type: oracledb.STRING },
        },
      };
      console.log("mini fetching query");
      const slipsQuery = await connection.execute(sql, binds, options);
      if (slipsQuery.rows.length > 0) {
        return slipsQuery.rows[0].POST_DATE;
      } else {
        return null;
      }
    } catch (err) {
      console.log(err);
    }
  }

  async minifetchInvoice(connection, invoice_sid) {
    const sql2 = `SELECT CMS.INVOICE.INVC_SID,
              
              CMS.INVOICE.INVC_NO,
              CMS.INVOICE.MODIFIED_DATE,
              CMS.INVOICE.CREATED_DATE,
              CMS.INVOICE.POST_DATE
              FROM CMS.INVOICE
              WHERE CMS.INVOICE.INVC_SID = `;
    let binds = {};
    let sql = sql2.concat(invoice_sid);
    try {
      const options = {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
        fetchInfo: {
          INVC_SID: { type: oracledb.STRING },
        },
      };
      const slipsQuery = await connection.execute(sql, binds, options);
      if (slipsQuery.rows.length > 0) {
        return slipsQuery.rows[0].POST_DATE;
      } else {
        console.log("failed");
        return null;
      }
    } catch (err) {
      console.log(err);
    }
  }

  async poll(connection, stores, bookmark?, startingDate?) {
    console.log("polling branch receipts");
    const dateTime = `${startingDate.substring(0, 10)} ${startingDate.substring(
      11,
      16
    )}:00`;
    for (let i = 0; i < stores.length; i++) {
      const currentStore = stores[i];
      console.log(
        `polling branch receipts for ${currentStore} / ${stores.length}`
      );
      try {
        let sql;
        let binds;
        let store_info = await await this.fetchStoreName(
          connection,
          currentStore,
          "store_code"
        );
        if (!bookmark) {
          const sql2 =
            `SELECT CMS.SLIP.SLIP_SID,
                           OUT_STORE_NO,
                           IN_STORE_NO,
                           CMS.SLIP.CREATED_DATE,
                           CMS.SLIP.MODIFIED_DATE,
                           CMS.SLIP.SLIP_NO,
                           CMS.SLIP_ITEM.ITEM_SID,
                           QTY,
                           PRICE,
                           COST,
                           CMS.LOT.LOT_NUMBER,
                           ALU,
                           CMS.LOT.EXPIRY_DATE
                           FROM CMS.SLIP
                           FULL JOIN CMS.SLIP_ITEM
                           ON CMS.SLIP.SLIP_SID = CMS.SLIP_ITEM.SLIP_SID
                           INNER JOIN CMS.INVENTORY_ALU_ALL
                           ON CMS.SLIP_ITEM.ITEM_SID = CMS.INVENTORY_ALU_ALL.ITEM_SID
                           INNER JOIN CMS.LOT
                           ON CMS.SLIP_ITEM.LOT_NUMBER = CMS.LOT.LOT_NUMBER
                           AND CMS.SLIP_ITEM.ITEM_SID  = CMS.LOT.ITEM_SID
                           WHERE (CMS.SLIP.OUT_STORE_NO = :v1 OR  CMS.SLIP.IN_STORE_NO = :v2)
                           AND CMS.SLIP.POST_DATE >= ` +
            `TO_DATE('${dateTime}', 'YYYY-MM-DD HH24:MI:SS')`; ////TODO: modify this to use OUT_STORE_NO, IN_STORE_NO,
          sql = sql2;
          binds = [store_info.store_no, store_info.store_no];
        } else {
          let bookMark = await this.minifetchSlip(connection, bookmark);
          sql = `SELECT CMS.SLIP.SLIP_SID,
                       OUT_STORE_NO,
                       IN_STORE_NO,
                       CMS.SLIP.CREATED_DATE,
                       CMS.SLIP.MODIFIED_DATE,
                       CMS.SLIP.SLIP_NO,
                       CMS.SLIP_ITEM.ITEM_SID,
                       QTY,
                       PRICE,
                       COST,
                       CMS.LOT.LOT_NUMBER,
                       ALU,
                       CMS.LOT.EXPIRY_DATE
                       FROM CMS.SLIP
                       FULL JOIN CMS.SLIP_ITEM
                       ON CMS.SLIP.SLIP_SID = CMS.SLIP_ITEM.SLIP_SID
                       INNER JOIN CMS.INVENTORY_ALU_ALL
                       ON CMS.SLIP_ITEM.ITEM_SID = CMS.INVENTORY_ALU_ALL.ITEM_SID
                       INNER JOIN CMS.LOT
                       ON CMS.SLIP_ITEM.LOT_NUMBER = CMS.LOT.LOT_NUMBER
                       AND CMS.SLIP_ITEM.ITEM_SID  = CMS.LOT.ITEM_SID
                       WHERE (CMS.SLIP.OUT_STORE_NO = :v1 OR  CMS.SLIP.IN_STORE_NO = :v2)
                       AND CMS.SLIP.POST_DATE      >= :v3`; //TODO: modify this to use OUT_STORE_NO, IN_STORE_NO,
          binds = [store_info.store_no, store_info.store_no, bookMark];
        }
        // For a complete list of options see the documentation.
        const options = {
          outFormat: oracledb.OUT_FORMAT_OBJECT,
          fetchInfo: {
            SLIP_SID: { type: oracledb.STRING },
          },
        };
        console.log("fetching query");
        const slipsQuery = await connection.execute(sql, binds, options);
        let orders = [];
        let reducedOrders = slipsQuery.rows.map((row) => ({
          sid: row.SLIP_SID,
          date: row.MODIFIED_DATE.getTime(),
        }));
        let reducedUniqueOrders = reducedOrders.reduce((unique, o) => {
          if (!unique.some((obj) => obj.sid === o.sid && obj.date === o.date)) {
            unique.push(o);
          }
          return unique;
        }, []);
        let orderDates = reducedUniqueOrders.map((row) => row.date);
        let uniqueItems = reducedUniqueOrders.map((row) => row.sid);
        let targetDateTime = Math.max(...orderDates);
        let targetIndex = orderDates.indexOf(targetDateTime);
        for (let i = 0; i < uniqueItems.length; i++) {
          const lines = slipsQuery.rows.filter(
            (row) => row.SLIP_SID.valueOf() == String(uniqueItems[i]).valueOf()
          );
          let singleLine = lines[0];
          if (singleLine == undefined) {
            console.log("reading failed, mismatched types in the filter");
          }

          const receiving_store = await await this.fetchStoreName(
            connection,
            singleLine.IN_STORE_NO,
            "store_no"
          );
          const sending_store = await await this.fetchStoreName(
            connection,
            singleLine.OUT_STORE_NO,
            "store_no"
          );

          let singleOrder = {
            slip_no: singleLine.SLIP_NO,
            slip_sid: singleLine.SLIP_SID,
            sending_store_code: sending_store.store_code,
            receiving_store_code: receiving_store.store_code,
            sending_store_name: sending_store.store_name,
            receiving_store_name: receiving_store.store_name,
            created_date: singleLine.CREATED_DATE,
            lines: lines,
          };
          orders.push(singleOrder);
        }
        let summary_lines = [];
        for (let j = 0; j < orders.length; j++) {
          let curr = orders[j];
          let line = {
            receiving_store_name: curr.store_name,
            order_no: curr.slip_no,
            number_of_lines: curr.lines.length,
          };
          summary_lines.push(line);
        }

        let summary = {
          number_of_orders: orders.length,
          summary_lines: summary_lines,
        };
        console.log(orders);
        console.log(summary);
        await axios
          .post("https://api.juleb.com/agent_receiver/transfer", orders)
          .then(async (response) => {
            console.log("sent:");
            console.log(summary);
            console.log(response);
            if (reducedOrders.length > 0) {
              let lastScannedVal = reducedUniqueOrders[targetIndex].sid;
              if (lastScannedVal.length > 0) {
                const bookmarks: IBookmark = JSON.parse(
                  (await promises.readFile(this.bookmarkFilePath)).toString()
                );
                bookmarks.transfer = lastScannedVal;
                await promises.writeFile(
                  this.bookmarkFilePath,
                  JSON.stringify(bookmarks)
                );
              }
            }
          });
      } catch (ex) {
        console.log(ex);
      }
    }
  }

  async pollPOSSales(connection, stores, bookmark?, startingDate?) {
    console.log("polling pos sales");

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
        if (!bookmark) {
          const sql2 =
            `SELECT CMS.INVOICE.INVC_SID,
                  STORE_NO,
                  CMS.INVOICE.CLERK_ID,
                  CASHIER_ID,
                  CMS.INVOICE.CREATED_DATE,
                  CMS.INVOICE.POST_DATE,
                  CMS.INVOICE.MODIFIED_DATE,
                  CMS.INVOICE.INVC_NO,
                  CMS.INVC_ITEM.ITEM_SID,
                  CMS.EMPLOYEE.EMPL_NAME,
                  QTY,
                  PRICE,
                  ORIG_PRICE,
                  ORIG_TAX_AMT,
                  TAX_CODE,
                  TAX_AMT,
                  TAX_PERC,
                  COST,
                  CMS.INVC_ITEM.LOT_NUMBER,
                  CMS.LOT.LOT_NUMBER,
                  ALU,
                  CMS.LOT.EXPIRY_DATE
                  FROM CMS.INVOICE
                  FULL JOIN CMS.INVC_ITEM
                  ON CMS.INVOICE.INVC_SID = CMS.INVC_ITEM.INVC_SID
                  INNER JOIN CMS.INVENTORY_ALU_ALL
                  ON CMS.INVC_ITEM.ITEM_SID = CMS.INVENTORY_ALU_ALL.ITEM_SID
                  INNER JOIN CMS.EMPLOYEE
                  ON CMS.INVOICE.CLERK_ID = CMS.EMPLOYEE.EMPL_ID
                  LEFT JOIN CMS.LOT
                  ON CMS.INVC_ITEM.LOT_NUMBER = CMS.LOT.LOT_NUMBER
                  AND CMS.INVC_ITEM.ITEM_SID  = CMS.LOT.ITEM_SID
                  WHERE CMS.INVOICE.STORE_NO = :v1
                  AND CMS.INVOICE.POST_DATE >= ` +
            `TO_DATE('${startingDate}', 'YYYY-MM-DD HH24:MI:SS') `;
          sql = sql2;
          binds = [store_info.store_no];
        } else {
          let bookMark = await this.minifetchInvoice(connection, bookmark);
          sql = `SELECT CMS.INVOICE.INVC_SID,
              STORE_NO,
              CMS.INVOICE.CLERK_ID,
              CASHIER_ID,
              CMS.INVOICE.CREATED_DATE,
              CMS.INVOICE.POST_DATE,
              CMS.INVOICE.MODIFIED_DATE,
              CMS.INVOICE.INVC_NO,
              CMS.INVC_ITEM.ITEM_SID,
              CMS.EMPLOYEE.EMPL_NAME,
              QTY,
              PRICE,
              ORIG_PRICE,
              ORIG_TAX_AMT,
              TAX_CODE,
              TAX_AMT,
              TAX_PERC,
              COST,
              CMS.INVC_ITEM.LOT_NUMBER,
              CMS.LOT.LOT_NUMBER,
              ALU,
              CMS.LOT.EXPIRY_DATE
              FROM CMS.INVOICE
              FULL JOIN CMS.INVC_ITEM
              ON CMS.INVOICE.INVC_SID = CMS.INVC_ITEM.INVC_SID
              INNER JOIN CMS.INVENTORY_ALU_ALL
              ON CMS.INVC_ITEM.ITEM_SID = CMS.INVENTORY_ALU_ALL.ITEM_SID
              INNER JOIN CMS.EMPLOYEE
              ON CMS.INVOICE.CLERK_ID = CMS.EMPLOYEE.EMPL_ID
              LEFT JOIN CMS.LOT
              ON CMS.INVC_ITEM.LOT_NUMBER = CMS.LOT.LOT_NUMBER
              AND CMS.INVC_ITEM.ITEM_SID  = CMS.LOT.ITEM_SID
                      WHERE CMS.INVOICE.POST_DATE      >= :v1 
                      AND CMS.INVOICE.STORE_NO = :v2`;
          binds = [bookMark, store_info.store_no];
        }
        const options = {
          outFormat: oracledb.OUT_FORMAT_OBJECT,
          fetchInfo: {
            INVC_SID: { type: oracledb.STRING },
            ITEM_SID: { type: oracledb.STRING },
          },
        };
        console.log("fetching pos sales");
        const slipsQuery = await connection.execute(sql, binds, options);
        let orders = [];

        let reducedOrders = slipsQuery.rows.map((row) => ({
          sid: row.INVC_SID,
          date: row.MODIFIED_DATE.getTime(),
        }));
        let reducedUniqueOrders = reducedOrders.reduce((unique, o) => {
          if (!unique.some((obj) => obj.sid === o.sid && obj.date === o.date)) {
            unique.push(o);
          }
          return unique;
        }, []);

        let orderDates = reducedUniqueOrders.map((row) => row.date);
        let uniqueItems = reducedUniqueOrders.map((row) => row.sid);
        let targetDateTime = Math.max(...orderDates);
        let targetIndex = orderDates.indexOf(targetDateTime);
        for (let i = 0; i < uniqueItems.length; i++) {
          const lines = slipsQuery.rows.filter(
            (row) => row.INVC_SID.valueOf() == String(uniqueItems[i]).valueOf()
          );
          let singleLine = lines[0];
          if (singleLine == undefined) {
            console.log("reading failed, mismatched types in the filter");
          }

          const paymentQuery = `SELECT CMS.INVC_TENDER.CRD_TYPE, CMS.INVC_TENDER.INVC_SID,CMS.INVC_TENDER.TENDER_TYPE, CMS.INVC_TENDER.AMT,CMS.CREDIT_CARD.CRD_NAME
        FROM CMS.INVC_TENDER
        LEFT JOIN CMS.CREDIT_CARD ON CMS.CREDIT_CARD.CRD_TYPE = CMS.INVC_TENDER.CRD_TYPE
        WHERE (CMS.CREDIT_CARD.SBS_NO = 8 OR CMS.CREDIT_CARD.SBS_NO IS NULL) AND
        CMS.INVC_TENDER.INVC_SID =  :v1`; ////////  AND CMS.CREDIT_CARD.SBS_NO = 8
          binds = [singleLine.INVC_SID];

          const paymentRows = await connection.execute(
            paymentQuery,
            binds,
            options
          );

          let singleOrder = {
            invoice_no: singleLine.INVC_NO,
            invoice_sid: singleLine.INVC_SID,
            store_no: store_info.store_no,
            created_date: singleLine.CREATED_DATE,
            store_name: store_info.store_name,
            store_code: currentStore,
            employee_code: singleLine.EMPL_NAME,
            lines: lines,
            payments: paymentRows.rows,
          };
          orders.push(singleOrder);
        }

        let summary_lines = [];
        for (let j = 0; j < orders.length; j++) {
          let curr = orders[j];
          let line = {
            store_name: curr.store_name,
            order_no: curr.invoice_no,
            number_of_lines: curr.lines.length,
          };
          summary_lines.push(line);
        }

        let summary = {
          number_of_orders: orders.length,
          summary_lines: summary_lines,
        };

        console.log(summary);
        await axios
          .post("https://api.juleb.com/agent_receiver/pos-invoices", orders)
          .then(async (response) => {
            console.log("sent:");
            console.log(summary);
            console.log(response);
            if (reducedOrders.length > 0) {
              let lastScannedVal = reducedUniqueOrders[targetIndex].sid;
              if (lastScannedVal.length > 0) {
                const bookmarks: IBookmark = JSON.parse(
                  (await promises.readFile(this.bookmarkFilePath)).toString()
                );
                bookmarks.pos = lastScannedVal;
                await promises.writeFile(
                  this.bookmarkFilePath,
                  JSON.stringify(bookmarks)
                ); // ONLY WRITE ON SUCCESSFUL RESPONSE FROM MICROSERVICE
              }
            }
          });
      } catch (ex) {
        console.log(ex);
      }
    }
  }

  async syncProducts(connection) {
    const currDate = new Date();
    const offset = 60 * 60 * 24 * 1000;
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
          WHERE CMS.INVN_SBS_PRICE.PRICE_LVL = 1
          AND CMS.INVN_SBS.CREATED_DATE >= TO_DATE('${formattedDate}', 'YYYY-MM-DD HH24:MI:SS')`; //
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
      .post("https://api.juleb.com/agent_receiver/master-data", payload)
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

    return;
    await axios
      .post("https://api.juleb.com/agent_receiver/master-data", payload)
      .then(async function (response) {
        console.log("sent:");
        console.log(masterDataQuery.rows);
        console.log(response);
      });
  }

  async wrapper() {
    const paramsExist = existsSync(this.paramsFilePath);
    const bookmarkExist = existsSync(this.bookmarkFilePath);
    if (!paramsExist || !bookmarkExist) {
      console.log(
        "make sure to have params.json and bookmark.json in the project directory"
      );
      return;
    }
    const params: IParams = JSON.parse(
      (await promises.readFile(this.paramsFilePath)).toString()
    );
    const bookmarks: IBookmark = JSON.parse(
      (await promises.readFile(this.bookmarkFilePath)).toString()
    );
    if (!params.storeCode) {
      console.log(
        "store code was not added to the params.json. Add it first please"
      );
      return;
    }
    console.log({ params });
    console.log({ bookmarks });
    const stores =
      typeof params.storeCode === "string"
        ? params.storeCode.split(",")
        : [params.storeCode];
    // console.log(stores);
    // return;
    const connection = await oracledb.getConnection({
      user: params.user ? params.user : "reportuser",
      password: params.password ? params.password : "report",
      connectString: params.connectString
        ? params.connectString
        : "localhost:1521/rproods",
    });
    if (params.syncPOS) {
      if (params.ignoreBookmark) {
        if (params.startingDate)
          await this.pollPOSSales(
            connection,
            stores,
            undefined,
            params.startingDate
          );
        else
          throw new Error(
            "you cannot ignore bookmark without specifying the starting date"
          );
      } else {
        if (bookmarks.pos)
          await this.pollPOSSales(connection, stores, bookmarks.pos);
        else throw new Error("no bookmark for POS");
      }
    }
    if (params.syncTransfers) {
      if (params.ignoreBookmark) {
        if (params.startingDate)
          await this.poll(connection, stores, undefined, params.startingDate);
        else
          throw new Error(
            "you cannot ignore bookmark without specifying the starting date"
          );
      } else {
        if (bookmarks.transfer)
          await this.poll(connection, stores, bookmarks.transfer);
        else throw new Error("no bookmark for Transfers");
      }
    }
    if (params.syncMasterData) {
      await this.syncProducts(connection);
      await this.syncPrices(connection);
    }
  }
}

const script = new Script();
script.wrapper();

interface IParams {
  user: string;
  password: string;
  connectString: string;
  storeCode: any;
  syncPOS: boolean;
  syncTransfers: boolean;
  syncMasterData: boolean;
  ignoreBookmark: boolean;
  startingDate: string;
}

interface IBookmark {
  pos: string;
  transfer: string;
}
