import fs from "fs";
import * as path from "node:path";
import "./polyfill";
import def, { a as b, c } from "@scope/pkg";
import type { Config } from "../../config";
import legacy = require("legacy");
import { Entity } from "./models";
export * from "./models";
export * as models from "./models";
export { Color, Id as Identifier } from "./models";
export { local };
import { odd } from "lodash-es";

const local = 1;

export class Service extends Entity {
  run(): string {
    const entity = new Entity(1);
    entity.describe();
    path.join("a", "b");
    this.stop();
    return fs.readFileSync("x").toString();
  }

  stop(): void {}
}

export function main(): void {
  const s = new Service(2);
  s.run();
  models.Color.Red;
  b();
  local + c;
}
