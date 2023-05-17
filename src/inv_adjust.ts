import axios from "axios";
import { existsSync, promises } from "fs";

class Script {
  paramsFilePath: string = "./params.json";
  julebApiUrl = "https://api.juleb.com/agent_receiver/retailpro";
  constructor() {}

  async wrapper() {
    const paramsExist = existsSync(this.paramsFilePath);
    if (!paramsExist) {
      console.log(
        "make sure to have params.json and bookmark.json in the project directory"
      );
      return;
    }
    const params = JSON.parse(
      (await promises.readFile(this.paramsFilePath)).toString()
    );
    if (!params.storeCode) {
      console.log(
        "store code was not added to the params.json. Add it first please"
      );
      return;
    }
    await axios
      .post(`${this.julebApiUrl}/zero-inv-adjustment`, {
        store_code: params.storeCode,
      })
      .then(async function (response) {
        console.log(response);
      });
  }
}

const script = new Script();
script.wrapper();
